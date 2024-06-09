from click.types import Tuple
import typer
import utils
import json

from pydantic import BaseModel
from typing import NamedTuple, Dict


class Workload(NamedTuple):
    is_epaxos: bool
    frac_writes: float
    theta: float

    def id(self):
        prot_str = "ep" if self.is_epaxos else "mp"
        write_str = f"{int(self.frac_writes * 100)}"
        theta_str = f"{int(self.theta* 100)}"
        return f"{prot_str}_{write_str}_{theta_str}"


LOCATION_TO_INDEX = {
    "ca": 0,
    "va": 1,
    "eu": 2,
    "or": 3,
    "jp": 4,
}


class GCloudClient:
    def __init__(self, ip, loc):
        self.ip = ip
        self.loc = loc
    @staticmethod
    def from_pulumi_output() -> tuple[str, Dict[str, "GCloudClient"]]:
        # get ips and locs from pulumi output
        pulumi_output = utils.execute(
            "pulumi stack output --json", "Fetching pulumi stack outputs"
        )
        pulumi_data = json.loads(pulumi_output())

        ips_locs = {
            "clients": {
                "eu": {
                    "private": pulumi_data["private_ip-client-eu"],
                    "public": pulumi_data["public_ip-client-eu"],
                },
                "or": {
                    "private": pulumi_data["private_ip-client-or"],
                    "public": pulumi_data["public_ip-client-or"],
                },
                "va": {
                    "private": pulumi_data["private_ip-client-va"],
                    "public": pulumi_data["public_ip-client-va"],
                },
            },
            "master": {
                "or": {
                    "private": pulumi_data["private_ip-master-or"],
                    "public": pulumi_data["public_ip-master-or"],
                },
            },
            "servers": {
                "eu": {
                    "private": pulumi_data["private_ip-server-eu"],
                    "public": pulumi_data["public_ip-server-eu"],
                },
                "or": {
                    "private": pulumi_data["private_ip-server-or"],
                    "public": pulumi_data["public_ip-server-or"],
                },
                "va": {
                    "private": pulumi_data["private_ip-server-va"],
                    "public": pulumi_data["public_ip-server-va"],
                },
            },
        }
        clients = {}
        for loc, ips in ips_locs["clients"].items():
            clients[loc] = GCloudClient((ips["public"], ips["private"]), loc)
        return (ips_locs["master"]["or"]["private"], clients)

    def id(self):
        return f"client-{self.loc}"

    def internal_ip(self):
        return self.ip[1]

    def external_ip(self):
        return self.ip[0]

    def zone(self):
        return {
            "ca": "us-west2-b",
            "va": "us-east4-a",
            "eu": "europe-west6-a",
            "or": "us-west1-b",
            "jp": "asia-northeast2-c",
        }[self.loc]

    def gssh(self, cmd, desc):
        # To see the commands that are run on each machine, uncomment the
        # statements below.
        print(cmd)
        # return lambda: None
        return utils.execute(
            self._gssh_cmd(cmd), "{}: {}".format(self.id(), desc)
        )

    def _gssh_cmd(self, cmd):
        if isinstance(cmd, list):
            cmd = "; ".join(cmd)

        return "gcloud compute ssh {} --zone {} --command='{}'".format(
            self.id(), self.zone(), cmd
        )

    def flags(self, master_ip, workload: Workload):
        zipfian_flags = f"-c -1 -theta {workload.theta}"
        flags = [
            f"-maddr {master_ip}",
            "-T 10",  # number of virtual clients
            f"-writes {workload.frac_writes}",
            zipfian_flags,
        ]
        if workload.is_epaxos:
            flags.append(f"-l {LOCATION_TO_INDEX[self.loc]}")
        return " ".join(flags)

    def run(self, master_ip, workload: Workload):
        flags = self.flags(master_ip, workload)
        client_command = (
            f"nohup epaxos/bin/client {flags} > output_{workload.id()}.txt 2>&1 &"
        )
        return self.gssh(client_command, f"Running client for {workload.id()}")

    def kill(self):
        kill_command = "killall client"
        return self.gssh(kill_command, "Killing all clients")

    def get_metrics(self, workload: Workload):
        metrics_command = "python3 epaxos/scripts/client_metrics.py"
        return self.gssh(metrics_command, f"Getting metrics for {workload.id()}")



class MetricsData(BaseModel):
    mean_lat_commit: float
    p50_lat_commit: float
    p90_lat_commit: float
    p95_lat_commit: float
    p99_lat_commit: float
    mean_lat_exec: float
    p50_lat_exec: float
    p90_lat_exec: float
    p95_lat_exec: float
    p99_lat_exec: float
    avg_tput: float
    total_ops: int

class WorkloadMetrics(BaseModel):
    clients: Dict[str, MetricsData]

class AllWorkloadsMetrics(BaseModel):
    workloads: Dict[str, WorkloadMetrics]

def main(is_epaxos: bool):
    master_ip, clients = GCloudClient.from_pulumi_output()
    print(master_ip)
    print(list((clients[loc].id(), clients[loc].ip) for loc in clients))

    # Populate all workloads metrics
    # all_workloads_metrics = AllWorkloadsMetrics(workloads={workload.id(): workload_metrics})
    all_workloads_metrics = AllWorkloadsMetrics(workloads={})

    for frac_writes in (x / 10 for x in range(0, 1)):
        for theta in (x / 10 for x in range(6, 7)):
            workload = Workload(
                is_epaxos=is_epaxos, frac_writes=frac_writes, theta=theta
            )
            print(workload)
            workload_metrics = WorkloadMetrics(clients={})
            for _, client in clients.items():
                # print("dummy run")
                output = client.run(master_ip, workload)
                print(output())
                metrics_output = client.get_metrics(workload)
                # get metrics
                metrics_data = json.loads(metrics_output())
                metrics_data = MetricsData(**metrics_data)
                workload_metrics.clients[client.id()] = metrics_data

                # Populate workload metrics
            all_workloads_metrics.workloads[workload.id()] = workload_metrics

    # Print the final metrics for verification
    with open('workload_metrics.json', 'w') as file:
        json.dump(all_workloads_metrics.model_dump(), file, indent=4)
    print("Workload metrics have been written to 'workload_metrics.json'")
    print(json.dumps(all_workloads_metrics.model_dump(), indent=4))




if __name__ == "__main__":
    typer.run(main)
