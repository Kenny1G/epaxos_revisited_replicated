"""A Google Cloud Python Pulumi program"""

import base64
from enum import Enum
from typing import Dict, List
import pulumi
import pulumi_gcp as gcp
from pulumi_command import remote, local
from pulumi.output import Output
from pulumi_gcp.compute.outputs import InstanceNetworkInterface
from pulumi_gcp.compute import (
    Image,
    Instance,
    InstanceBootDiskInitializeParamsArgs,
    InstanceBootDiskArgs,
    InstanceNetworkInterfaceArgs,
    InstanceNetworkInterfaceAccessConfigArgs,
)

# from pulumi_gcp import storage

# Import the program's configuration settings.
config = pulumi.Config()
machine_type = config.get("machineType", "n1-standard-1")
image_family = config.get("imageFamily", "ubuntu-pro-1804-lts")
image_project = config.get("imageProject", "ubuntu-os-pro-cloud")
epaxos_dir = config.get(
    "epaxosDir", "/Users/kennyosele/Documents/Projects/epaxos_revisited"
)
gcp_project = config.get("gcpProject", "cs244-423515")
private_key_b64 = None
# Derived values for instance naming
LOCATION_TO_INDEX = {
    "ca": 0,
    "va": 1,
    "eu": 2,
    "or": 3,
    "jp": 4,
}
# locs = ["or", "eu", "jp"]  # List of locations for your instances
locs = ["or"]  # List of locations for your instances


class Zone(Enum):
    CA = "us-west2-b"
    VA = "us-east4-a"
    EU = "europe-west6-a"
    OR = "us-west1-b"
    JP = "asia-northeast2-c"


def zone(loc):
    return {
        "ca": "us-west2-b",
        "va": "us-east4-a",
        "eu": "europe-west6-a",
        "or": "us-west1-b",
        "jp": "asia-northeast2-c",
    }[loc]


def server_name(loc):
    return f"server-{loc}"


def client_name(loc):
    return f"client-{loc}"


# Create instances
servers: Dict[str, Instance] = {}
clients: Dict[str, Instance] = {}

boot_disk = InstanceBootDiskArgs(
    initialize_params=InstanceBootDiskInitializeParamsArgs(
        # image=f"projects/{image_project}/global/images/family/{image_family}",
        image="https://www.googleapis.com/compute/beta/projects/ubuntu-os-pro-cloud/global/images/ubuntu-pro-1804-bionic-v20240516"
    ),
)
network_interfaces = [
    InstanceNetworkInterfaceArgs(
        access_configs=[InstanceNetworkInterfaceAccessConfigArgs()],
        network="default",
    )
]

setup_script = """
#!/bin/bash

#Install golang
sudo apt-get purge golang -y
sudo add-apt-repository ppa:longsleep/golang-backports -y
sudo apt-get update -y
sudo apt-get install golang-go -y



#Download packages
export GOPATH=~/epaxos_revisited
go get golang.org/x/sync/semaphore
go get -u google.golang.org/grpc
go get -u github.com/golang/protobuf/protoc-gen-go
go get -u github.com/VividCortex/ewma
export PATH=$PATH:$GOPATH/bin
# For client metrics script
sudo apt-get install python3-pip -y && pip3 install numpy

"""


def rsync(loc: str):
    sshopts = "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
    rsync_command = (
        'rsync --delete --exclude-from "{f}/.gitignore" '
        '-re "{sshopts}" {f} {remote}:~'
    )
    server_remote_str: Output[str] = (
        servers[loc].network_interfaces[0].access_configs[0].nat_ip
    )
    server_rsync = server_remote_str.apply(
        lambda str: local.Command(
            "resource_server-rsync",
            create=rsync_command.format(
                sshopts=sshopts,
                f=epaxos_dir,
                remote=str,
            ),
        )
    )

    client_remote_str: Output[str] = (
        clients[loc].network_interfaces[0].access_configs[0].nat_ip
    )
    client_rsync = client_remote_str.apply(
        lambda str: local.Command(
            "resource_client-rsync",
            create=rsync_command.format(
                sshopts=sshopts,
                f=epaxos_dir,
                remote=str,
            ),
        )
    )
    pulumi.export("rsync", server_rsync.stdout)


def install(loc: str, first: bool = False):
    the_key = private_key_b64 if private_key_b64 else ""
    install_command = ""
    if first:
        install_command += (
            "go get golang.org/x/sync/semaphore && "
            "go get -u google.golang.org/grpc && "
            "go get -u github.com/golang/protobuf/protoc-gen-go && "
            "go get -u github.com/VividCortex/ewma && "
            "export PATH=$PATH:$GOPATH/bin && "
        )
    install_command += (
        "export GOPATH=~/epaxos_revisited && "
        "export GO111MODULE=off && "
        "go clean && "
        "go install master && "
        "go install server && "
        "go install client"
    )
    print(install_command)

    server_remote_str: Output[str] = (
        servers[loc].network_interfaces[0].access_configs[0].nat_ip
    )
    client_remote_str: Output[str] = (
        clients[loc].network_interfaces[0].access_configs[0].nat_ip
    )
    server_remote_str.apply(
        lambda str: remote.Command(
            "resource_client-install",
            connection=remote.ConnectionArgs(
                host=server_remote_str,
                user="kennyosele",
                private_key=base64.b64decode(the_key).decode("utf-8"),
            ),
            create=install_command,
        )
    )

    # connection_client: remote.ConnectionArgs = remote.ConnectionArgs(
    #     host=clients[loc].network_interfaces[0].access_configs[0].nat_ip,
    #     user="kennyosele",
    #     private_key=base64.b64decode(the_key).decode("utf-8"),
    # )


for loc in locs:
    # Spin up google compute engine instances, install GO and download packages
    servers[loc] = Instance(
        f"resource_{server_name(loc)}",
        network_interfaces=network_interfaces,
        name=server_name(loc),
        machine_type=machine_type,
        zone=zone(loc),
        boot_disk=boot_disk,
        metadata_startup_script=setup_script,
    )

    clients[loc] = Instance(
        f"resource_{client_name(loc)}",
        network_interfaces=network_interfaces,
        name=client_name(loc),
        machine_type=machine_type,
        zone=zone(loc),
        boot_disk=boot_disk,
        metadata_startup_script=setup_script,
    )
    rsync(loc)
    install(loc, first=False)

# Expose Ports (Firewall Rules)
# mock_client_rule = gcp.compute.Firewall(
#     "mock-client",
#     allows=[
#         gcp.compute.FirewallAllowArgs(
#             protocol="tcp",
#             ports=["7000-8000"],
#         ),
#     ],
#     source_ranges=["0.0.0.0/0"],
#     source_tags=[s.name for s in servers.values()],
# )

# clock_sync_ui_rule = gcp.compute.Firewall(
#     "clock-sync-ui",
#     allows=[
#         gcp.compute.FirewallAllowArgs(
#             protocol="tcp",
#             ports=["9001"],
#         ),
#     ],
#     source_ranges=["0.0.0.0/0"],
#     source_tags=[servers[locs[0]].name],  # Assuming the first server is the master
# )

# Export the ip addresses of compute instances
for loc in locs:
    pulumi.export(
        f"{server_name(loc)}_public_ip",
        servers[loc].network_interfaces[0].access_configs[0].nat_ip,
    )
    pulumi.export(
        f"{client_name(loc)}_public_ip",
        clients[loc].network_interfaces[0].access_configs[0].nat_ip,
    )
