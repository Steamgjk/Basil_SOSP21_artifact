import os
import subprocess
from subprocess import PIPE, Popen
import time
import ruamel.yaml
from termcolor import colored
import argparse


TAG = "devour"
LOGIN_PATH = "/home/ubuntu"
SSH_KEY = "/home/ubuntu/.ssh/jk-rsa.pem"
ssh_identity = '-i {}'.format(SSH_KEY) if SSH_KEY else ''
# Prefix for SSH and SCP.
SSH = 'ssh {} -q -o ConnectTimeout=2 -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no '.format(
    ssh_identity)
SCP = 'scp -r {} -q -o ConnectTimeout=2 -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no '.format(
    ssh_identity)
USERNAME = "ubuntu"
CMD_RETRY_TIMES = 3


def generate_ttcs_cfg_file(internal_ip, is_reference=False, use_ntp=False):
    if is_reference:
        content_str = '''management_address: "InternalIP"
log_dir: "/var/opt/ttcs/log"
subscription_mode: true
coordinator_address: "c-gjk1994gjk1994-c89e.gcp.clockwork.io"
coordinator_subscription_service_port: 6176
probe_address: "InternalIP"
clock_quality: 10
correct_clock: false'''
        cfg_file = content_str.replace("InternalIP", internal_ip)
        cfg_file_name = "ttcs-agent.cfg"
        with open(cfg_file_name, "w") as f:
            f.write(cfg_file)
        f.close()
        return cfg_file_name
    else:
        if use_ntp:
            content_str = '''management_address: "InternalIP"
log_dir: "/var/opt/ttcs/log"
subscription_mode: true
coordinator_address: "c-gjk1994gjk1994-c89e.gcp.clockwork.io"
coordinator_subscription_service_port: 6176
probe_address: "InternalIP"
clock_quality: 1
correct_clock: false'''
        else:
            content_str = '''management_address: "InternalIP"
log_dir: "/var/opt/ttcs/log"
subscription_mode: true
coordinator_address: "c-gjk1994gjk1994-c89e.gcp.clockwork.io"
coordinator_subscription_service_port: 6176
probe_address: "InternalIP"
clock_quality: 1
correct_clock: true'''
        cfg_file = content_str.replace("InternalIP", internal_ip)
        cfg_file_name = "ttcs-agent.cfg"
        with open(cfg_file_name, "w") as f:
            f.write(cfg_file)
        f.close()
        return cfg_file_name




def retry_proc_error(procs_list):
    procs_error = []
    for server, proc, cmd in procs_list:
        output, err = proc.communicate()
        if proc.returncode != 0:
            proc = Popen(cmd.split(), stdout=PIPE, stderr=PIPE)
            procs_error.append((server, proc, cmd))
    return procs_error


def start_ttcs_node(internal_ip, is_reference, use_ntp=False):
    clean_prev_deb_cmd = "sudo dpkg -P ttcs-agent"
    run_command([internal_ip], clean_prev_deb_cmd, in_background=False)
    install_deb_cmd = "sudo dpkg -i /home/steam1994/ttcs-agent_1.0.21_amd64.deb"
    #install_deb_cmd = "sudo dpkg -i /root/ttcs-agent_1.0.12_amd64.deb"
    run_command([internal_ip], install_deb_cmd, in_background=False)

    cfg_file = generate_ttcs_cfg_file(internal_ip, is_reference, use_ntp)
    local_file_path = "./ttcs-agent.cfg"
    remote_dir = "/etc/opt/ttcs"
    remote_path = remote_dir + "/ttcs-agent.cfg"

    chmod_cmd = "sudo chmod -R 777 {remote_dir}".format(remote_dir=remote_dir)
    run_command([internal_ip], chmod_cmd, in_background=False)

    rm_cmd = "sudo rm -f {remote_path}".format(remote_path=remote_path)
    run_command([internal_ip], rm_cmd, in_background=False)

    scp_files([internal_ip], local_file_path, remote_path, to_remote=True)

    if is_reference is not True and use_ntp is False:
        stop_ntp_cmd = "sudo systemctl stop ntp"
        run_command([internal_ip], stop_ntp_cmd, in_background=False)
        disable_ntp_cmd = "sudo systemctl disable ntp"
        run_command([internal_ip], disable_ntp_cmd, in_background=False)
        stop_ntp_cmd = "sudo systemctl stop chronyd"
        run_command([internal_ip], stop_ntp_cmd, in_background=False)
        disable_ntp_cmd = "sudo systemctl disable chronyd"
        run_command([internal_ip], disable_ntp_cmd, in_background=False)
    else:
        enable_ntp_cmd = "sudo systemctl enable chronyd"
        run_command([internal_ip], enable_ntp_cmd, in_background=False)
        start_ntp_cmd = "sudo systemctl start chronyd"
        run_command([internal_ip], start_ntp_cmd, in_background=False)

    sys_start_ttcp_agent_cmd = "sudo systemctl start ttcs-agent"
    run_command([internal_ip], sys_start_ttcp_agent_cmd, in_background=False)


def launch_ttcs(server_ip_list):
    stop_ntp_cmd = "sudo systemctl stop chronyd"
    run_command(server_ip_list, stop_ntp_cmd, in_background=False)
    disable_ntp_cmd = "sudo systemctl disable chronyd"
    run_command(server_ip_list, disable_ntp_cmd, in_background=False)
    stop_ntp_cmd = "sudo systemctl stop ntp"
    run_command(server_ip_list, stop_ntp_cmd, in_background=False)
    disable_ntp_cmd = "sudo systemctl disable ntp"
    run_command(server_ip_list, disable_ntp_cmd, in_background=False)
    sys_start_ttcp_agent_cmd = "sudo systemctl start ttcs-agent"
    run_command(server_ip_list, sys_start_ttcp_agent_cmd, in_background=False)



def scp_files(server_ip_list, local_path_to_file, remote_dir, to_remote):
    '''
    copies the file in 'local_path_to_file' to the 'remote_dir' in all servers
    whose external ip addresses are in 'server_ip_list'

    args
        server_ip_list: list of external IP addresses to communicate with
        local_path_to_file: e.g. ./script.py
        remote_dir: e.g. ~
        to_remote: whether to copy to remote (true) or vice versa (false)
    returns
        boolean whether operation was succesful on all servers or not
    '''
    src = remote_dir if not to_remote else local_path_to_file
    src_loc = 'remote' if not to_remote else 'local'
    dst = remote_dir if to_remote else local_path_to_file
    dst_loc = 'remote' if to_remote else 'local'

    message = 'from ({src_loc}) {src} to ({dst_loc}) {dst}'.format(
        src_loc=src_loc, src=src, dst_loc=dst_loc, dst=dst)
    print('---- started scp {}'.format(message))

    procs = []
    for server in server_ip_list:
        if to_remote:
            cmd = '{} {} {}@{}:{}'.format(SCP, local_path_to_file,
                                          USERNAME, server, remote_dir)
            proc = Popen(cmd.split(), stdout=PIPE, stderr=PIPE)
        else:
            cmd = '{} {}@{}:{} {}'.format(SCP, USERNAME, server,
                                          remote_dir, local_path_to_file)
            proc = Popen(cmd.split(), stdout=PIPE, stderr=PIPE)
        # print("scp cmd ", cmd)
        procs.append((server, proc, cmd))

    success = True
    procs_error = retry_proc_error(procs)
    retries = 1
    while retries < CMD_RETRY_TIMES and procs_error:
        procs_error = retry_proc_error(procs)
        retries += 1

    if retries >= CMD_RETRY_TIMES and procs_error:
        success = False
        for server, proc, cmd in procs_error:
            output, err = proc.communicate()
            if proc.returncode != 0:
                print(
                    colored('[{}]: FAIL SCP - [{}]'.format(server, cmd),
                            'yellow'))
                print(colored('Error Response:', 'blue', attrs=['bold']),
                      proc.returncode, output, err)

    if success:
        print(
            colored('---- SUCCESS SCP {} on {}'.format(message,
                                                       str(server_ip_list)),
                    'green',
                    attrs=['bold']))
    else:
        print(
            colored('---- FAIL SCP {}'.format(message), 'red', attrs=['bold']))
    return success


def run_command(server_ip_list, cmd, in_background=True):
    '''
    runs the command 'cmd' in all servers whose external ip addresses are
    in 'server_ip_list'

    cfg
        server_ip_list: list of external IP addresses to communicate with
        cmd: command to run
    returns
        boolean whether operation was succesful on all servers or not
    '''
    if not in_background:
        print('---- started to run command - [{}] on {}'.format(
            cmd, str(server_ip_list)))
    else:
        print(
            colored('---- started to run [IN BACKGROUND] command - [{}] on {}'.
                    format(cmd, str(server_ip_list)),
                    'blue',
                    attrs=['bold']))
    procs = []
    for server in server_ip_list:
        ssh_cmd = '{} {}@{} {}'.format(SSH, USERNAME, server, cmd)
        proc = Popen(ssh_cmd.split(), stdout=PIPE, stderr=PIPE)
        procs.append((server, proc, ssh_cmd))

    success = True
    output = ''
    if not in_background:
        procs_error = retry_proc_error(procs)
        retries = 1
        while retries < CMD_RETRY_TIMES and procs_error:
            procs_error = retry_proc_error(procs)
            retries += 1

        if retries >= CMD_RETRY_TIMES and procs_error:
            success = False
            for server, proc, cmd in procs_error:
                output, err = proc.communicate()
                if proc.returncode != 0:
                    print(
                        colored(
                            '[{}]: FAIL run command - [{}]'.format(
                                server, cmd), 'yellow'))
                    print(colored('Error Response:', 'blue', attrs=['bold']),
                          proc.returncode, output, err)

        if success:
            print(
                colored('---- SUCCESS run command - [{}] on {}'.format(
                    cmd, str(server_ip_list)),
                        'green',
                        attrs=['bold']))
        else:
            print(
                colored('---- FAIL run command - [{}]'.format(cmd),
                        'red',
                        attrs=['bold']))

    return success, output



