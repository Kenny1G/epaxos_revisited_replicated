"""A Google Cloud Python Pulumi program"""

import base64
from enum import Enum
from typing import Dict, List
import pulumi
import pulumi_gcp as gcp
from pulumi_command import remote, local
from pulumi.output import Output
from pulumi.resource import ResourceOptions
from pulumi_gcp.compute.outputs import InstanceNetworkInterface
from pulumi_gcp.compute import (
    Image,
    Instance,
    InstanceBootDiskInitializeParamsArgs,
    InstanceBootDiskArgs,
    InstanceNetworkInterfaceArgs,
    InstanceNetworkInterfaceAccessConfigArgs,
)
import pulumiverse_time as time

#################################
# Pulumi Configuration Settings #
#################################
# Import the program's configuration settings.
config = pulumi.Config()
gcp_config = pulumi.Config("gcp")
machine_type = config.get("machineType", "n1-standard-1")
image_family = config.get("imageFamily", "ubuntu-pro-1804-lts")
image_project = config.get("imageProject", "ubuntu-os-pro-cloud")
epaxos_dir = config.get("epaxosDir", "/Users/kennyosele/Documents/Projects/epaxos")
gcp_project = gcp_config.get("project", "cs244-423515")
unix_username = config.get("unixUsername", "kennyosele")
# Private key for SSH access into the instances to run commands remotely
# Associated public key must be added to the project's metadata
private_key_b64 = private_key_b64 = config.get(
    "privateKeyB64",
    "LS0tLS1CRUdJTiBPUEVOU1NIIFBSSVZBVEUgS0VZLS0tLS0KYjNCbGJuTnphQzFyWlhrdGRqRUFBQUFBQkc1dmJtVUFBQUFFYm05dVpRQUFBQUFBQUFBQkFBQUFNd0FBQUF0emMyZ3RaVwpReU5UVXhPUUFBQUNCMHdUUkN4TmNNQlhDdEtvQmtyR29hR0NkcjdqaURVLzIzY3dUcFVsWDVxd0FBQUtBZHVnSFdIYm9CCjFnQUFBQXR6YzJndFpXUXlOVFV4T1FBQUFDQjB3VFJDeE5jTUJYQ3RLb0JrckdvYUdDZHI3amlEVS8yM2N3VHBVbFg1cXcKQUFBRUE0bEhHZFFQWm9haGdNRFNYRFNPWllqcEJkellhbXpHWVNoTFYxZ1BkZnhIVEJORUxFMXd3RmNLMHFnR1NzYWhvWQpKMnZ1T0lOVC9iZHpCT2xTVmZtckFBQUFGMnRsYm01NU1XZEFZM011YzNSaGJtWnZjbVF1WldSMUFRSURCQVVHCi0tLS0tRU5EIE9QRU5TU0ggUFJJVkFURSBLRVktLS0tLQ==",
)

# Derived values for instance naming
LOCATION_TO_INDEX = {
    "ca": 0,
    "va": 1,
    "eu": 2,
    "or": 3,
    "jp": 4,
}
# locs = ["or", "eu", "jp", "ca", "va"]  # List of locations for your instances
# List of locations where instances will be deployed
locs = ["or"]


#######################################################
# Constants for creating and configuring VM instances #
#######################################################
# --- Create instances ---
# Data structures to track created resources
servers: Dict[str, Instance] = {}
clients: Dict[str, Instance] = {}

# fields we need to create a pulumi instance resource
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

SETUP_SCRIPT = "/usr/local/bin/setup_epaxos.sh"
epaxos_folder_name = epaxos_dir.split("/")[-1] if epaxos_dir else "epaxos"
go_path = f"~/{epaxos_folder_name}"

# Script that installs golang, the third party packages
setup_script = f"""
#!/bin/bash

#Install golang
sudo apt-get purge golang-go -y
sudo apt-get update -y
curl -OL https://go.dev/dl/go1.11.2.linux-amd64.tar.gz
tar xvf go1.11.2.linux-amd64.tar.gz
sudo chown -R root:root ./go
sudo mv go /usr/local
# For client metrics script
sudo apt-get install python3-pip -y && pip3 install numpy


# Write commands to a script in the home directory
cat << 'EOF' > {SETUP_SCRIPT}
#!/bin/bash

export GOPATH={go_path}
export PATH=$PATH:$GOPATH/bin:/usr/local/go/bin
go get golang.org/x/sync/semaphore
go get -u google.golang.org/grpc
go get -u github.com/golang/protobuf/protoc-gen-go
go get -u github.com/VividCortex/ewma
EOF

# Make the script executable
chmod +x {SETUP_SCRIPT}
sudo chown $(whoami):$(whoami) {SETUP_SCRIPT}
"""


DELAY_DURATION = "1s"


#############################################################
# Helper Functions for creating and configuring VM instances#
#############################################################


# function to get the gcp zone string of a location
def zone(loc):
    return {
        "ca": "us-west2-b",
        "va": "us-east4-a",
        "eu": "europe-west6-a",
        "or": "us-west1-b",
        "jp": "asia-northeast2-c",
    }[loc]


# function for consistent naming of servers and clients resources
def server_name(loc):
    return f"server-{loc}"


def client_name(loc):
    return f"client-{loc}"


# function to rsync the epaxos directory to newly created VM instances
def rsync(loc: str):
    # generic variables for rsyncing to both server and client
    sshopts = "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
    rsync_command = (
        'rsync --delete --exclude-from "{f}/.gitignore" '
        '-re "{sshopts}" {f} {remote}:~'
    )

    # rsync the epaxos directory to the server at this location
    wait30_seconds = time.Sleep(
        f"time_rsync_delay-{server_name(loc)}",
        create_duration=DELAY_DURATION,
        opts=ResourceOptions(depends_on=[servers[loc]]),
    )
    server_remote_str: Output[str] = (
        servers[loc].network_interfaces[0].access_configs[0].nat_ip
    )
    server_rsync = server_remote_str.apply(
        lambda str: local.Command(
            f"command_rsync-{server_name(loc)}",
            create=rsync_command.format(
                sshopts=sshopts,
                f=epaxos_dir,
                remote=str,
            ),
            opts=ResourceOptions(depends_on=[wait30_seconds]),
        )
    )

    # rsync the epaxos directory to the client at this location
    wait30_seconds = time.Sleep(
        f"time_rsync_delay-{client_name(loc)}",
        create_duration=DELAY_DURATION,
        opts=ResourceOptions(depends_on=[clients[loc]]),
    )
    client_remote_str: Output[str] = (
        clients[loc].network_interfaces[0].access_configs[0].nat_ip
    )
    client_rsync = client_remote_str.apply(
        lambda str: local.Command(
            f"command_rsync-{client_name(loc)}",
            create=rsync_command.format(
                sshopts=sshopts,
                f=epaxos_dir,
                remote=str,
            ),
            opts=ResourceOptions(depends_on=[wait30_seconds]),
        )
    )

    return (client_rsync, server_rsync)


# Function to install the third party packages and epaxos binaries on the VM instances
def install(loc: str, depends_on: List[Output], first: bool = False):
    the_key = private_key_b64 if private_key_b64 else ""
    install_command = ""
    if first:
        install_command += "whoami && " f"ls /usr/local/bin && " f"{SETUP_SCRIPT} &&"
    install_command += (
        "export PATH=$PATH:/usr/local/go/bin && "
        f"export GOPATH={go_path}&& "
        "go clean && "
        "go install master && "
        "go install server && "
        "go install client"
    )

    def remote_install_command(remote_str: str, name: str):
        return remote.Command(
            f"command_install-{name}",
            connection=remote.ConnectionArgs(
                host=remote_str,
                user=unix_username,
                private_key=base64.b64decode(the_key).decode("utf-8"),
            ),
            create=install_command,
            opts=ResourceOptions(depends_on=depends_on),
        )

    servers[loc].network_interfaces[0].access_configs[0].nat_ip.apply(
        lambda str: remote_install_command(str, server_name(loc))
    )
    clients[loc].network_interfaces[0].access_configs[0].nat_ip.apply(
        lambda str: remote_install_command(str, client_name(loc))
    )


def create_instance(loc: str, name: str):
    return Instance(
        f"instance-{name}",
        network_interfaces=network_interfaces,
        name=name,
        machine_type=machine_type,
        zone=zone(loc),
        boot_disk=boot_disk,
        metadata_startup_script=setup_script,
    )


###################################################################
# Declarative Code for creating VM instances and configuring them #
###################################################################

for loc in locs:
    # Create VMs
    servers[loc] = create_instance(loc, server_name(loc))
    clients[loc] = create_instance(loc, client_name(loc))
    # Run Rsync locally to copy the epaxos directory to the VM instances
    server_rsync, client_rsync = rsync(loc)
    # Run necessary commands remotely to install third party packages and epaxos binaries on the VM instances
    install(loc, [server_rsync, client_rsync], first=True)

# TODO(kenny): Right now, I have the firewall rules already created in my Project.
# Need to port that over to Pulumi.
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
