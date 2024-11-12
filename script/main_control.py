from ssh_control import *
from aws_control import *
from IPython import embed

AMI_NAME = "devour-img-v1"
SECURITY_GROUP_NAME = "all-allowed"

## LAN
LAN_REGION = "us-west-1"
LAN_VPC_ID = "vpc-0924e2fce2c8e1dfe"
LAN_SUBNET_ID = "subnet-0bb0fca0c34333454"
LAN_AMI_ID = "ami-0ff98caab8b49d363"
LAN_SG_ID = "sg-0190b1902ca3bdf10"

SERVER_TAG = f"{TAG}-lan-server"
CLIENT_TAG = f"{TAG}-lan-client"

def query_instance_info(server_regions):
  vpc_ids = []
  subnet_ids = []
  security_group_ids = []
  ami_ids = []
  for region in server_regions:
    subnet_details = list_subnets(region)
    vpc_id = None
    subnet_id = None
    for subnet_detail in subnet_details:
        if subnet_detail["VPCName"] is not None and subnet_detail["VPCName"].startswith("devour-vpc-"):
            vpc_id = subnet_detail["VPCID"]
            subnet_id = subnet_detail["SubnetID"]
            break
    if vpc_id is None or subnet_id is None:
      print(colored('Error:', 'red', attrs=['bold']), "no valid vpc/subnet ", region )
    vpc_ids.append(vpc_id)

    sg_id = None
    sg_details = list_security_groups(region)
    for sg_detail in sg_details:
        if sg_detail["SecurityGroupName"] == SECURITY_GROUP_NAME:
          sg_id = sg_detail["SecurityGroupID"]
          break
    if sg_id is None:
      print(colored('Error:', 'red', attrs=['bold']), "no valid security group ", region )
    security_group_ids.append(sg_id)

    ami_id = None
    ami_details = list_amis(region)
    for ami_detail in ami_details:
        if ami_detail["AMIName"] == AMI_NAME:
          ami_id = ami_detail["AMIID"]
          break
    if ami_id is None:
      print(colored('Error:', 'red', attrs=['bold']), "no valid AMI ", region )
    ami_ids.append(ami_id)

  return vpc_ids, subnet_ids, security_group_ids, ami_ids

def create_config(config_name, replication_factor, num_shards, num_replicas, server_name_to_detail):
    f = open(config_name, "w")
    f.write(f"f {replication_factor}\n")
    port_cnt = 8000
    for r in range(num_replicas):
      f.write("group\n")
      for s in range(num_shards):
        server_name = f"{SERVER_TAG}-s-{s}-r-{r}"
        instance_detail = server_name_to_detail[server_name]
        server_ip = instance_detail["PrivateIP"]
        f.write(f"replica {server_ip}:{port_cnt}\n")
        port_cnt += 1
    f.close()

def list_all_files(folder_path):
    """List all files in a folder and its subdirectories."""
    file_paths = []
    for root, _, files in os.walk(folder_path):
        for file in files:
            file_paths.append(os.path.join(root, file))
    return file_paths

def copy_keys(dst_ips):
    rm_cmd = "sudo rm -rf keys"
    run_command(dst_ips, rm_cmd)
    scp_files(dst_ips, f"{LOGIN_PATH}/key.tar.gz", "key.tar.gz", to_remote=True)
    unzip_cmd = "tar -zxvf key.tar.gz"
    run_command(dst_ips, unzip_cmd, in_background=False)
    mv_cmd = "mv Basil_SOSP21_artifact/src/keys keys"
    run_command(dst_ips, mv_cmd, in_background=False)
    # folder_path =  f"{LOGIN_PATH}/Basil_SOSP21_artifact/src/keys"
    # all_keys = list_all_files(folder_path)
    # for key_file in all_keys:
    #    file_name = key_file.split("/")[-1]
    #    scp_files(dst_ips, key_file, f"keys/{file_name}", to_remote=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--num_shards',  type=int, default = 1,
                        help='Specify the number of replicas ')
    parser.add_argument('--num_replicas',  type=int, default = 6,
                        help='Specify the number of replicas ')
    parser.add_argument('--num_clients',  type=int, default = 10,
                        help='Specify the number of clients ')
    parser.add_argument('--replication_factor',  type=int, default = 1,
                        help='Specify the replication_factor ')
    args = parser.parse_args()

    replication_f = args.replication_factor
    num_shards = args.num_shards
    num_replicas = args.num_replicas
    num_clients = args.num_clients

    print("shards: ", num_shards)
    print("replicas: ", num_replicas)
    print("clients: ", num_clients)

    ## WAN Related
    # server_regions = ["us-west-1", "eu-north-1", "ap-southeast-1",
    #                   "ap-northeast-2","ap-northeast-1","ap-southeast-2",
    #                   "sa-east-1"
    #                   ]

    # vpc_ids, subnet_ids, security_group_ids, ami_ids = query_instance_info(server_regions)


    # # LAN Related
    # ## Create LAN Servers
    # for s in range(num_shards):
    #    for r in range(num_replicas):
    #       instance_detail = {
    #          "InstanceName": f"{SERVER_TAG}-s-{s}-r-{r}",
    #          "Region": LAN_REGION,
    #          "PrivateIP": f"193.168.44.{10+s*20+r}",
    #          "VPCID": LAN_VPC_ID,
    #          "SubnetID": LAN_SUBNET_ID
    #       }
    #       create_instance(instance_detail, LAN_AMI_ID, LAN_SG_ID, instance_type="m5.2xlarge")
    #       print(colored('Info:', 'green', attrs=['bold']), f"Server created  S-{s}-R-{r}" )


    # for c in range(num_clients):
    #     instance_detail = {
    #         "InstanceName": f"{CLIENT_TAG}-{c}",
    #         "Region": LAN_REGION,
    #         "PrivateIP": f"193.168.44.{160+c}",
    #         "VPCID": LAN_VPC_ID,
    #         "SubnetID": LAN_SUBNET_ID
    #     }
    #     create_instance(instance_detail, LAN_AMI_ID, LAN_SG_ID, instance_type="m5.2xlarge")
    #     print(colored('Info:', 'green', attrs=['bold']), f"Client created  C-{c}" )

    # exit(0)

    server_name_to_detail = {}
    client_name_to_detail = {}
    instance_data = list_instances([LAN_REGION])
    for instance_detail in instance_data:
       if instance_detail["SubNetID"] == LAN_SUBNET_ID:
          if instance_detail["InstanceName"].startswith(SERVER_TAG):
             name = instance_detail["InstanceName"]
             server_name_to_detail[name] = instance_detail
          if instance_detail["InstanceName"].startswith(CLIENT_TAG):
             name = instance_detail["InstanceName"]
             client_name_to_detail[name] = instance_detail


    server_ips = [server_name_to_detail[key]["PrivateIP"]
                  for key in server_name_to_detail.keys() ]
    client_ips = [client_name_to_detail[key]["PrivateIP"]
                  for key in client_name_to_detail.keys() ]
    print(server_ips)
    print(client_ips)
    server_ids_and_regions = [
        { "InstanceId": server_name_to_detail[key]["InstanceID"],
          "Region": server_name_to_detail[key]["Region"]
        } for key in server_name_to_detail.keys()
    ]
    client_ids_and_regions = [
        { "InstanceId": client_name_to_detail[key]["InstanceID"],
          "Region": client_name_to_detail[key]["Region"]
        } for key in client_name_to_detail.keys()
    ]

    # start_instances(server_ids_and_regions, region=LAN_REGION)
    # start_instances(client_ids_and_regions, region=LAN_REGION)
    # copy_keys(server_ips+client_ips)

    # exit(0)

    # Copy Binary
    server_binary = f"{LOGIN_PATH}/Basil_SOSP21_artifact/src/store/server"
    client_binary = f"{LOGIN_PATH}/Basil_SOSP21_artifact/src/store/benchmark/async/benchmark"

    # scp_files(server_ips, server_binary, "server", to_remote=True )
    # scp_files(client_ips, client_binary, "benchmark", to_remote=True )

    num_client_threads= 1

    for protocol in ["Basil"]:
        protocol_name = None
        client_protocol_name = None
        if protocol == "Tapir":
          num_replicas = 2 * replication_f + 1
          protocol_name = "tapir"
          client_protocol_name = "txn-l"
        elif protocol == "Basil":
          protocol_name = "indicus"
          client_protocol_name = "indicus"

        clean_cmd = "pkill -9 server benchmark"
        run_command(server_ips+client_ips, clean_cmd, in_background=False)
        # Copy Config
        config_name = "config"
        create_config(config_name, replication_f, num_shards, num_replicas, server_name_to_detail)
        scp_files(server_ips+client_ips, config_name, config_name, to_remote=True)

        for s in range(num_shards):
            for r in range(num_replicas):
              server_name = f"{SERVER_TAG}-s-{s}-r-{r}"
              server_ip = server_name_to_detail[server_name]["PrivateIP"]
              server_cmd = f"./server --config_path config --group_idx {s} --num_groups {num_replicas} --num_shards {num_shards} --replica_idx {r} --protocol {protocol_name} --num_keys 1000000 --debug_stats --indicus_key_path ./keys > {server_name}.log 2>&1 &"
              run_command([server_ip], server_cmd, in_background=True)
        time.sleep(10)

        for c in range(num_clients):
           client_name = f"{CLIENT_TAG}-{c}"
           client_ip = server_name_to_detail[server_name]["PrivateIP"]
           client_cmd = f"./benchmark --config_path config --num_groups {num_replicas} --num_shards {num_shards} --num_clients {num_client_threads} --protocol_mode {client_protocol_name} --num_keys 1000000 --benchmark rw --num_ops_txn 2 --exp_duration 30 --num_requests 10000 --client_id {c} --warmup_secs 0 --cooldown_secs 0 --key_selector zipf --zipf_coefficient 0.5 --indicus_key_path ./keys > {client_name}.log 2>&1 &"
           run_command([client_ip], client_cmd, in_background=True)
        time.sleep(40)


        # Copy Stats File
        folder_name = f"{protocol}-{num_clients}-{num_client_threads}"
        stats_folder = f"{LOGIN_PATH}/{folder_name}"
        mkdir_cmd = f"sudo mkdir -p -m 777 {stats_folder}"
        os.system(mkdir_cmd)

        for c in range(num_clients):
           client_name = f"{CLIENT_TAG}-{c}"
           client_ip = server_name_to_detail[server_name]["PrivateIP"]
           for cc in range(num_client_threads):
              idx = int(c * num_client_threads + cc)
              csv_file = f"Client-{idx}"
              scp_files([client_ip],f"{stats_folder}/{csv_file}", csv_file, to_remote=False)


    # shut_cmd = "sudo shutdown now"
    # run_command(server_ips+client_ips, shut_cmd, in_background=False)


    # instance_list = []
    # instance_ids = []
    # for instance_detail in instance_data:
    #    if instance_detail["SubNetID"] == LAN_SUBNET_ID:
    #       instance_list.append(instance_detail)
    #       instance_ids.append(instance_detail["InstanceID"])
    # print(instance_ids)
    # print("instance number ", len(instance_list))
    # delete_instance(instance_ids, region = LAN_REGION)

    # instance_id_and_regions = [
    #    { "InstanceId": instance_detail["InstanceID"],
    #       "Region": instance_detail["Region"]
    #     }
    #    for instance_detail in instance_list
    # ]
    # stop_instances(instance_id_and_regions)

    # modify_instance_type(instance_id_and_regions, "m5.2xlarge")



    # subnet_details = list_subnets("us-west-1")
    # sg_details = list_security_groups("us-west-1")
    # ami_details = list_amis("us-west-1")
    # embed()
    # instance_details = {
    #     "InstanceName": "devour-lan-s-00-r-00",
    #     "Region": "us-west-1",
    #     "PrivateIP": "172.31.17.1",
    #     "VPCID": "vpc-02faabd7e3e5fd917",
    #     "SubnetID": "subnet-03b21dd7c5f1c67db",
    #     "SecurityGroupID": "sg-0fdd429872c501300"  # Security Group ID is now part of instance_details
    # }
    # ami_id = "ami-0ff98caab8b49d363"  # Replace with the correct AMI ID
    # security_group_id = "sg-0f452f3125d55f9ae"
    # instance_info = create_instance(instance_details, ami_id, security_group_id)
    # print(instance_info)
    # instances_list = list_instances(["us-west-1"])
    # name_list = [instance["InstanceName"] for instance in instances_list]
    # ip_list = [instance["PrivateIP"] for instance in instances_list]
    # print(name_list)
    # print(ip_list)
    # delete_instance("i-0adfdbb0bd5e6f860", "us-west-1")
    # list_amis()
    # sg_groups = list_security_groups()
    # # print(sg_groups)
    # for sg_group in sg_groups:
    #     if(sg_group["SecurityGroupName"]=="all-allowed"):
    #         print(sg_group)
