from ssh_control import *
from aws_control import *



if __name__ == "__main__":
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
    instances_list = list_instances(["us-west-1"])
    name_list = [instance["InstanceName"] for instance in instances_list]
    ip_list = [instance["PrivateIP"] for instance in instances_list]
    print(name_list)
    print(ip_list)
    # delete_instance("i-0adfdbb0bd5e6f860", "us-west-1")
    # list_amis()
    # sg_groups = list_security_groups()
    # # print(sg_groups)
    # for sg_group in sg_groups:
    #     if(sg_group["SecurityGroupName"]=="all-allowed"):
    #         print(sg_group)
