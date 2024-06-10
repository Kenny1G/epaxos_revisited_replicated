"""A Google Cloud Python Pulumi program"""

import base64
import pulumi
import utils
from typing import List, NamedTuple
from pulumi import Output, ComponentResource, StackReference
import pulumiverse_time as time
from pulumi_command import remote, local
from pulumi.resource import ResourceOptions
from pulumi_gcp.compute import (
    Instance,
    InstanceBootDiskInitializeParamsArgs,
    InstanceBootDiskArgs,
    InstanceNetworkInterfaceArgs,
    InstanceNetworkInterfaceAccessConfigArgs,
)

DEV_STACK = "kenny1g/epaxos_revisited_replicated/dev"

TENK = 1e4
HUNDK = 1e5
MILLION = 1e6
TENMIL = 1e7
HUNDMIL = 1e8
BILLION = 1e9

LARGE_INT_TO_DESC = {
    TENK: "10K",
    HUNDK: "100K",
    MILLION: "1M",
    TENMIL: "10M",
    HUNDMIL: "100M",
    BILLION: "1B",
}

LOCATION_TO_INDEX = {
    "ca": 0,
    "va": 1,
    "eu": 2,
    "or": 3,
    "jp": 4,
}
SETUP_SCRIPT_PATH = "/usr/local/bin/setup_epaxos.sh"
VM_IMAGE_URL = "https://www.googleapis.com/compute/beta/projects/ubuntu-os-pro-cloud/global/images/ubuntu-pro-1804-bionic-v20240516"


if pulumi.get_stack() != "dev":
    dev = StackReference(DEV_STACK)


class GCloudInstance:
    def id(self):
        raise NotImplementedError()

    def ip(self):
        if pulumi.get_stack() == "dev":
            if self.instance_resource is None:
                raise ValueError(
                    f"""
                    instance has not been created on {self.loc}
                    """
                )
            return self.instance_resource.network_interfaces[0].access_configs[0].nat_ip
        else:
            return dev.get_output(f"public_ip-{self.id()}")

    def internal_ip(self):
        if pulumi.get_stack() == "dev":
            if self.instance_resource is None:
                raise ValueError(
                    f"""
                    instance has not been created on {self.loc}
                    """
                )
            return self.instance_resource.network_interfaces[0].network_ip
        else:
            return dev.get_output(f"private_ip-{self.id()}")

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
        self.run_resource = None
        self.metrics_resource = None

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

    def run_remote_command(
        self,
        resource_prefix,
        command,
        host_str,
        delete_command=None,
        this_resource=None,
        extra_depends_on=[],
        parent_resource=None,
    ):
        return remote.Command(
            f"{resource_prefix}-{self.id()}",
            connection=remote.ConnectionArgs(
                host=host_str,
                user=self.unix_username,
                private_key=base64.b64decode(self.private_key_b64).decode("utf-8"),
            ),
            create=command,
            delete=delete_command,
            opts=ResourceOptions(
                depends_on=[
                    r
                    for r in [
                        self.install_resource,
                        self.rsync_resource,
                        self.instance_resource,
                    ]
                    + extra_depends_on
                    if r is not None and r != this_resource
                ],
                parent=parent_resource,
            ),
        )

    def run_go_installs(self):
        if self.go_path is None:
            raise ValueError("go_path is not set")
        if self.private_key_b64 is None:
            raise ValueError("private_key_b64 needs to be set in pulumi config")

        run_setup_script = f"""\
until {SETUP_SCRIPT_PATH}
do
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
        self.install_resource = self.ip().apply(
            lambda host_str: self.run_remote_command(
                "command_install",
                install_command,
                host_str,
                this_resource=self.install_resource,
            )
        )

    def run_rsync(self):
        sshopts = "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
        rsync_command = (
            'rsync --delete --exclude-from "{f}/.gitignore" '
            '-re "{sshopts}" {f} {remote}:~'
        )
        self.rsync_resource = self.ip().apply(
            lambda str: local.Command(
                f"command_rsync-{self.id()}",
                create=rsync_command.format(
                    sshopts=sshopts,
                    f=self.epaxos_dir,
                    remote=str,
                ),
                opts=ResourceOptions(
                    depends_on=[r for r in [self.instance_resource] if r is not None]
                ),
            )
        )

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
            self.ip(),
        )
        pulumi.export(
            f"private_ip-{name}",
            self.internal_ip(),
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

    def run(self, master_ip_output, master_run_resource):
        def lambda_helper(internal_ip, external_ip, master_ip):
            port = 7070 + LOCATION_TO_INDEX[self.loc]
            # flags = f" -port {port} -maddr {master_ip} -addr {internal_ip} -e " # -e for epaxos
            flags = f" -port {port} -maddr {master_ip} -addr {internal_ip} "
            server_command = "nohup epaxos/bin/server {} > output.txt 2>&1 &".format(
                flags
            )
            delete_command = "kill $(pidof epaxos/bin/server)"
            return self.run_remote_command(
                "run_command",
                server_command,
                external_ip,
                delete_command=delete_command,
                extra_depends_on=[master_run_resource],
            )

        self.run_resource = Output.all(
            self.internal_ip(), self.ip(), master_ip_output
        ).apply(lambda args: lambda_helper(*args))


class Workload(NamedTuple):
    is_epaxos: bool
    frac_writes: float
    theta: float

    def id(self):
        prot_str = "ep" if self.is_epaxos else "mp"
        write_str = f"{int(self.frac_writes * 100)}"
        theta_str = f"{int(self.theta* 100)}"
        return f"{prot_str}_{write_str}_{theta_str}"


class WorkloadRun(ComponentResource):
    def __init__(self, name, master, clients, workload, opts=None):
        super().__init__("WorkloadRun", name, None, opts)

        outputs = {}
        for client in list(clients.values())[2:]:
            clnt_run = client.run(master.internal_ip(), workload, parent=self)
            self.clnt_run = client.run_resource

        for client in list(clients.values())[2:]:
            self.mname, self.mval = client.get_metrics(workload, parent=self)
            outputs[self.mname] = self.mval

        self.time = time.Sleep(
            f"time_{self.mname}",
            create_duration="2s",
            opts=pulumi.ResourceOptions(parent=self, depends_on=[self.clnt_run]),
        )
        self.register_outputs(outputs)


class GCloudClient(GCloudInstance):
    def id(self):
        return f"client-{self.loc}"

    def flags(self, master_ip, is_epaxos=True, frac_writes=0.5, theta=0.9):
        zipfian_flags = f"-c -1 -theta {theta}"
        flags = [
            f"-maddr {master_ip}",
            f"-T 10",  # number of virtual clients
            f"-writes {frac_writes}",
            zipfian_flags,
        ]
        if is_epaxos:
            flags.append(f"-l {LOCATION_TO_INDEX[self.loc]}")
        return " ".join(flags)

    def run(self, master_ip_output, workload: Workload, parent=None):
        def lambda_helper(internal_ip, external_ip, master_ip):
            flags = self.flags(
                master_ip,
                is_epaxos=workload.is_epaxos,
                frac_writes=workload.frac_writes,
                theta=workload.theta,
            )
            client_command = f"nohup epaxos/bin/client {flags} > output.txt 2>&1 &"

            delete_command = "kill $(pidof epaxos/bin/client)"
            return self.run_remote_command(
                f"command_run_{workload.id()}",
                client_command,
                external_ip,
                delete_command=delete_command,
                # extra_depends_on=dependencies,
                parent_resource=parent,
            )

        self.run_resource = Output.all(
            self.internal_ip(), self.ip(), master_ip_output
        ).apply(lambda args: lambda_helper(*args))

    def get_metrics(self, workload: Workload, parent: WorkloadRun):
        metrics_command = "python3 epaxos/scripts/client_metrics.py"
        self.metrics_resource = self.ip().apply(
            lambda ip: self.run_remote_command(
                f"command_metrics_{workload.id()}",
                metrics_command,
                ip,
                extra_depends_on=[self.run_resource],
                parent_resource=parent,
            )
        )
        pulumi.export(
            f"metrics_{workload.id()}-{self.id()}", self.metrics_resource.stdout
        )
        return (
            f"metrics_{workload.id()}-{self.id()}",
            self.metrics_resource.stdout,
        )


class GCloudMaster(GCloudInstance):
    def __init__(self, config, loc):
        super().__init__(config, loc)

    def id(self):
        return f"master-{self.loc}"

    def run_master(self, server_instances: List[GCloudServer]):
        master_command = (
            "nohup epaxos/bin/master -N {len_ips} -ips {ips} > moutput.txt 2>&1 &"
        )

        self.run_resource = Output.all(
            self.ip(),
            *[server.internal_ip() for server in server_instances],
        ).apply(
            lambda ips: self.run_remote_command(
                "run_command",
                master_command.format(len_ips=len(ips) - 1, ips=",".join(ips[1:])),
                ips[0],
                delete_command="kill $(pidof epaxos/bin/master)",
            )
        )


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

    def run_and_get_metrics(self):
        pulumi.info("Running master...")
        self.master.run_master(list(self.servers.values()))

        pulumi.info("Running servers...")
        for server in self.servers.values():
            server.run(self.master.internal_ip(), self.master.run_resource)
        server_runs = [server.run_resource for server in self.servers.values()]

        # pulumi.info("Running clients...")
        # is_epaxos = True
        # depends_on = server_runs + [self.master.run_resource]
        # for frac_writes in (x / 10 for x in range(0, 2)):
        #     for theta in (x / 10 for x in range(6, 8)):
        #         workload = Workload(
        #             is_epaxos=is_epaxos, frac_writes=frac_writes, theta=theta
        #         )
        #         workload_run = WorkloadRun(
        #             workload.id(),
        #             self.master,
        #             self.clients,
        #             workload,
        #             opts=ResourceOptions(depends_on=depends_on),
        #         )
        #         depends_on = workload_run.time


# Main execution
config = pulumi.Config()
deployment = EPaxosDeployment(config)
stack = pulumi.get_stack()
if stack == "dev":
    deployment.deploy()
if stack == "experiments":
    deployment.run_and_get_metrics()
