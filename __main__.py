"""A Google Cloud Python Pulumi program"""

import base64
import pulumi
from typing import List
from pulumi import Output
from pulumi_command import remote, local
from pulumi.resource import ResourceOptions
from pulumi_gcp.compute import (
    Instance,
    InstanceBootDiskInitializeParamsArgs,
    InstanceBootDiskArgs,
    InstanceNetworkInterfaceArgs,
    InstanceNetworkInterfaceAccessConfigArgs,
)


SETUP_SCRIPT_PATH = "/usr/local/bin/setup_epaxos.sh"
VM_IMAGE_URL = "https://www.googleapis.com/compute/beta/projects/ubuntu-os-pro-cloud/global/images/ubuntu-pro-1804-bionic-v20240516"


class GCloudInstance:
    def __init__(self, config, loc):
        self.config = config
        self.loc = loc
        self.machine_type = config.get("machineType", "n1-standard-1")
        self.image_family = config.get("imageFamily", "ubuntu-pro-1804-lts")
        self.image_project = config.get("imageProject", "ubuntu-os-pro-cloud")
        self.epaxos_dir = config.get(
            "epaxosDir", "/Users/kennyosele/Documents/Projects/epaxos"
        )
        self.unix_username = config.get("unixUsername", "kennyosele")
        self.private_key_b64 = config.get(
            "privateKeyB64",
            "LS0tLS1CRUdJTiBPUEVOU1NIIFBSSVZBVEUgS0VZLS0tLS0KYjNCbGJuTnphQzFyWlhrdGRqRUFBQUFBQkc1dmJtVUFBQUFFYm05dVpRQUFBQUFBQUFBQkFBQUFNd0FBQUF0emMyZ3RaVwpReU5UVXhPUUFBQUNCMHdUUkN4TmNNQlhDdEtvQmtyR29hR0NkcjdqaURVLzIzY3dUcFVsWDVxd0FBQUtBZHVnSFdIYm9CCjFnQUFBQXR6YzJndFpXUXlOVFV4T1FBQUFDQjB3VFJDeE5jTUJYQ3RLb0JrckdvYUdDZHI3amlEVS8yM2N3VHBVbFg1cXcKQUFBRUE0bEhHZFFQWm9haGdNRFNYRFNPWllqcEJkellhbXpHWVNoTFYxZ1BkZnhIVEJORUxFMXd3RmNLMHFnR1NzYWhvWQpKMnZ1T0lOVC9iZHpCT2xTVmZtckFBQUFGMnRsYm01NU1XZEFZM011YzNSaGJtWnZjbVF1WldSMUFRSURCQVVHCi0tLS0tRU5EIE9QRU5TU0ggUFJJVkFURSBLRVktLS0tLQ==",
        )

        self.go_path = None
        self.setup_script = self.create_setup_script()

        self.instance_resource = None
        self.rsync_resource = None
        self.install_resource = None

    def create_setup_script(self):
        epaxos_folder_name = (
            self.epaxos_dir.split("/")[-1] if self.epaxos_dir else "epaxos"
        )
        self.go_path = f"~/{epaxos_folder_name}"
        return f"""
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
cat << 'EOF' > {SETUP_SCRIPT_PATH}
#!/bin/bash

export GOPATH={self.go_path}
export PATH=$PATH:$GOPATH/bin:/usr/local/go/bin
go get golang.org/x/sync/semaphore
go get -u google.golang.org/grpc
go get -u github.com/golang/protobuf/protoc-gen-go
go get -u github.com/VividCortex/ewma
EOF

# Make the script executable
chmod +x {SETUP_SCRIPT_PATH}
sudo chown $(whoami):$(whoami) {SETUP_SCRIPT_PATH}
"""

    def run_go_installs(self):
        if self.go_path is None:
            raise ValueError("go_path is not set")
        if self.rsync_resource is None:
            raise ValueError(
                f"rsync has not been run on instance: {self.id()}, did create_instance() fail?"
            )
        if self.private_key_b64 is None:
            raise ValueError("private_key_b64 needs to be set in pulumi config")

        run_setup_script = f"""\
until {SETUP_SCRIPT_PATH}
do
    echo "Try again"
    sleep 2
done
"""
        install_command = (
            f"$({run_setup_script}) && "
            "export PATH=$PATH:/usr/local/go/bin && "
            f"export GOPATH={self.go_path} && "
            "go clean && "
            "go install master && "
            "go install server && "
            "go install client"
        )
        self.install_resource = (
            self.instance_resource.network_interfaces[0]
            .access_configs[0]
            .nat_ip.apply(
                lambda str: remote.Command(
                    f"command_install-{self.id()}",
                    connection=remote.ConnectionArgs(
                        host=str,
                        user=self.unix_username,
                        private_key=base64.b64decode(self.private_key_b64).decode(
                            "utf-8"
                        ),
                    ),
                    create=install_command,
                    opts=ResourceOptions(depends_on=[self.rsync_resource]),
                )
            )
        )
        pulumi.export(
            f"output_run_go_installs-{self.id()}", self.install_resource.stdout
        )

    def run_rsync(self):
        sshopts = "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
        rsync_command = (
            'rsync --delete --exclude-from "{f}/.gitignore" '
            '-re "{sshopts}" {f} {remote}:~'
        )
        remote_str = (
            self.instance_resource.network_interfaces[0].access_configs[0].nat_ip
        )
        self.rsync_resource = remote_str.apply(
            lambda str: local.Command(
                f"command_rsync-{self.id()}",
                create=rsync_command.format(
                    sshopts=sshopts,
                    f=self.epaxos_dir,
                    remote=str,
                ),
                opts=ResourceOptions(depends_on=[self.instance_resource]),
            )
        )
        pulumi.export(f"output_run_rsync-{self.id()}", self.rsync_resource.stdout)

    def create_instance(self):
        name = self.id()

        boot_disk = InstanceBootDiskArgs(
            initialize_params=InstanceBootDiskInitializeParamsArgs(
                image=VM_IMAGE_URL,
            ),
        )
        network_interfaces = [
            InstanceNetworkInterfaceArgs(
                access_configs=[InstanceNetworkInterfaceAccessConfigArgs()],
                network="default",
            )
        ]
        self.instance_resource = Instance(
            f"instance_{name}",
            network_interfaces=network_interfaces,
            name=name,
            machine_type=self.machine_type,
            zone=self.zone(),
            boot_disk=boot_disk,
            metadata_startup_script=self.setup_script,
        )
        pulumi.export(
            f"public_ip-{name}",
            self.instance_resource.network_interfaces[0].access_configs[0].nat_ip,
        )

    def zone(self):
        return {
            "ca": "us-west2-b",
            "va": "us-east4-a",
            "eu": "europe-west6-a",
            "or": "us-west1-b",
            "jp": "asia-northeast2-c",
        }[self.loc]


class GCloudServer(GCloudInstance):
    def id(self):
        return f"server-{self.loc}"


class GCloudMaster(GCloudInstance):
    def __init__(self, config, loc):
        super().__init__(config, loc)

    def id(self):
        return f"master-{self.loc}"

    def run_master(self, server_instances: List[GCloudServer]):
        if self.install_resource is None:
            raise ValueError("Master instance is not ready")
        master_command = (
            "cd epaxos && " "bin/master -N {len_ips} -ips {ips} > moutput.txt 2>&1 &"
        )
        output_run_master = Output.all(
            self.instance_resource.network_interfaces[0].access_configs[0].nat_ip,
            *[
                server.instance_resource.network_interfaces[0].network_ip
                for server in server_instances
            ]
        ).apply(
            lambda ips: remote.Command(
                "command_run-master",
                connection=remote.ConnectionArgs(
                    host=ips[0],
                    user=self.unix_username,
                    private_key=base64.b64decode(self.private_key_b64).decode("utf-8"),
                ),
                create=master_command.format(len_ips=len(ips) - 1, ips=",".join(ips[1:])),
                opts=ResourceOptions(depends_on=[self.install_resource]),
            )
        )

        pulumi.export("output_run-master", output_run_master.stdout)


class GCloudClient(GCloudInstance):
    def id(self):
        return f"client-{self.loc}"


class EPaxosDeployment:
    def __init__(self, config, locs=["or", "eu", "va"]):
        self.config = config
        self.locs = locs
        self.servers = {loc: GCloudServer(config, loc) for loc in self.locs}
        self.clients = {loc: GCloudClient(config, loc) for loc in self.locs}
        self.master = GCloudMaster(config, self.locs[0])

    def deploy(self):
        def deploy_instance(instance):
            instance.create_instance()
            instance.run_rsync()
            instance.run_go_installs()

        deploy_instance(self.master)
        for loc in self.locs:
            deploy_instance(self.servers[loc])
            deploy_instance(self.clients[loc])

    def run(self):
        # while self.master.install_resource is None:
        #     pulumi.log.info("Waiting for installation resources to be ready...")
        #     pulumi.runtime.sleep(10)
        self.master.run_master(self.servers.values())


# Main execution
config = pulumi.Config()
deployment = EPaxosDeployment(config)
deployment.deploy()
deployment.run()
