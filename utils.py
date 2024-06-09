import psutil
import subprocess
import sys
def execute(cmd, desc):
    """
    Runs 'command' as a shell process, returning a function handler that will
    wait for the process to complete when called. 'desc' provides identifying
    information about the command.
    """
    if isinstance(cmd, list):
        cmd = '; '.join(cmd)

    p = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        shell=True,
        executable='/bin/bash',
    )
    return lambda: complete_process(p, desc)

def complete_process(process, desc):
    """
    Waits for 'process', a shell process, to complete. Returns the stdout of the
    process. If the process returns an error code, prints the stderr of the
    process. 'desc' provides identifying information about the command, and is
    printed in the case of an error.
    """
    p = psutil.Process(process.pid)
    children = p.children(recursive=True)

    out, err = process.communicate()
    retcode = process.returncode
    out = out.strip()

    if retcode != 0:
        err = err.strip()
        if err:
            print('ERROR when completing process "{}": {}'.format(desc, err),
            file=sys.stderr)

    for cp in children:
        if psutil.pid_exists(cp.pid):
            cp.kill()

    del process
    return out
