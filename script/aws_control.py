import boto3


def start_instances(instances, region= None):
    """
    Start EC2 instances based on a list of instance IDs and regions.

    Parameters:
    instances (list): A list of dictionaries with 'instance_id' and 'region' keys.
    """
    if region is None:
      for instance in instances:
          instance_id = instance['InstanceId']
          region = instance['Region']

          # Initialize the EC2 client for the specific region
          ec2_client = boto3.client('ec2', region_name=region)

          # Start the instance
          ec2_client.start_instances(InstanceIds=[instance_id])
          waiter = ec2_client.get_waiter('instance_running')
          waiter.wait(InstanceIds=[instance_id])
          print(f"Started instance {instance_id} in region {region}")
    else:
        instance_ids = [instance_detail["InstanceId"] for instance_detail in instances]
        ec2_client = boto3.client('ec2', region_name=region)
        # Start the instance
        ec2_client.start_instances(InstanceIds=instance_ids)
        waiter = ec2_client.get_waiter('instance_running')
        waiter.wait(InstanceIds=instance_ids)
        print(f"Started instances")

def stop_instances(instances):
    """
    Stop EC2 instances based on a list of instance IDs and regions.

    Parameters:
    instances (list): A list of dictionaries with 'instance_id' and 'region' keys.
    """
    for instance in instances:
        instance_id = instance['InstanceId']
        region = instance['Region']

        # Initialize the EC2 client for the specific region
        ec2_client = boto3.client('ec2', region_name=region)

        # Stop the instance
        ec2_client.stop_instances(InstanceIds=[instance_id])
        waiter = ec2_client.get_waiter('instance_stopped')
        waiter.wait(InstanceIds=[instance_id])
        print(f"Stopped instance {instance_id} in region {region}")


def list_instances(regions = None):
    # Initialize EC2 client to list all available regions
    ec2_client = boto3.client('ec2')

    # Get list of all regions
    if regions is None:
        regions = [region['RegionName'] for region in ec2_client.describe_regions()['Regions']]

    # Initialize an empty list to store instance information
    instances_data = []

    # Iterate through each region and collect instance details
    for region in regions:
        # Create an EC2 resource for each region
        ec2 = boto3.resource('ec2', region_name=region)

        # Retrieve all instances in the region
        instances = ec2.instances.all()

        # Iterate over each instance
        for instance in instances:
            # Extract the Name tag if it exists
            name_tag = next((tag['Value'] for tag in instance.tags if tag['Key'] == 'Name'), 'No Name')
            private_ip = instance.private_ip_address or "No Private IP"
            instance_state = instance.state['Name']
            vpc_id = instance.vpc_id or "No VPC ID"
            instance_id = instance.instance_id
            subnet_id = instance.subnet_id or "No Subnet ID"
            # Append instance details as a dictionary to the list
            instances_data.append({
                "InstanceName": name_tag,
                "Region": region,
                "PrivateIP": private_ip,
                "State": instance_state,
                "VPCID": vpc_id,
                "InstanceID": instance_id,
                "SubNetID":subnet_id
            })

    # Display the collected instance data
    for instance_info in instances_data:
        print(instance_info)
    return instances_data

def list_amis(region):
    # Initialize EC2 client
    ec2 = boto3.client('ec2', region_name = region)

    # Initialize an empty list to store AMI information
    ami_data = []

    # Retrieve AMIs in the region (you can specify filters if needed, or retrieve all)
    amis = ec2.describe_images(Owners=['self'])['Images']  # Only AMIs owned by the user

    # Iterate over each AMI and collect the relevant details
    for ami in amis:
        ami_name = ami.get('Name', 'No Name')
        ami_id = ami['ImageId']

        # Append AMI details as a dictionary to the list
        ami_data.append({
            "Region": region,
            "AMIName": ami_name,
            "AMIID": ami_id
        })

    # # Display the collected AMI data
    # for ami_info in ami_data:
    #     print(ami_info)

    return ami_data


def list_security_groups(region):
    # Initialize EC2 client
    ec2 = boto3.client('ec2', region_name = region)

    # Initialize an empty list to store security group details
    security_groups_list = []

    # Retrieve security groups in the region
    response = ec2.describe_security_groups()

    # Iterate over each security group and collect details
    for sg in response['SecurityGroups']:
        sg_name = sg['GroupName']
        sg_id = sg['GroupId']

        # Append security group details as a dictionary to the list
        security_groups_list.append({
            "Region": region,
            "SecurityGroupName": sg_name,
            "SecurityGroupID": sg_id
        })

    return security_groups_list




def list_subnets(region):
    """
    List all subnets in a specified AWS region with details on VPC and subnet names and IDs.

    Parameters:
    region (str): The AWS region to query for subnets.

    Returns:
    list: A list of dictionaries, each containing VPC name, VPC ID, subnet name, and subnet ID.
    """
    ec2_client = boto3.client('ec2', region_name=region)
    subnet_details = []

    # Fetch all VPCs in the region
    vpcs = ec2_client.describe_vpcs()['Vpcs']
    # Map VPC IDs to VPC names (if they have a Name tag)
    vpc_name_map = {vpc['VpcId']: next((tag['Value'] for tag in vpc.get('Tags', []) if tag['Key'] == 'Name'), None)
                    for vpc in vpcs}

    # Fetch all subnets in the region
    subnets = ec2_client.describe_subnets()['Subnets']

    for subnet in subnets:
        # Extract VPC ID and subnet ID
        vpc_id = subnet['VpcId']
        subnet_id = subnet['SubnetId']

        # Retrieve VPC and subnet names from tags (if available)
        vpc_name = vpc_name_map.get(vpc_id)
        subnet_name = next((tag['Value'] for tag in subnet.get('Tags', []) if tag['Key'] == 'Name'), None)

        # Append the subnet information to the list
        subnet_details.append({
            "VPCName": vpc_name,
            "VPCID": vpc_id,
            "SubnetName": subnet_name,
            "SubnetID": subnet_id
        })

    return subnet_details


def create_instance(instance_details, ami_id, security_group_id, instance_type="t2.micro"):
    """
    Create an EC2 instance based on the provided details and security group ID.

    Parameters:
    instance_details (dict): A dictionary containing 'InstanceName', 'Region', 'PrivateIP', 'VPCID', 'SubnetID'.
    ami_id (str): The AMI ID to launch the instance from.
    security_group_id (str): The ID of the security group to associate with the instance.
    instance_type (str): The type of EC2 instance to launch (default is 't2.micro').

    Returns:
    dict: A dictionary containing the instance ID and other relevant details.
    """
    instance_name = instance_details.get("InstanceName")
    region = instance_details.get("Region")
    private_ip = instance_details.get("PrivateIP")
    vpc_id = instance_details.get("VPCID")
    subnet_id = instance_details.get("SubnetID")

    # Initialize EC2 client for the specified region
    ec2_client = boto3.client('ec2', region_name=region)

    # Launch the EC2 instance
    response = ec2_client.run_instances(
        ImageId=ami_id,
        InstanceType=instance_type,
        MinCount=1,
        MaxCount=1,
        NetworkInterfaces=[{
            'AssociatePublicIpAddress': False,
            'SubnetId': subnet_id,
            'PrivateIpAddress': private_ip,
            'DeviceIndex': 0,  # Primary network interface device index
            'Groups': [security_group_id]
        }],
        TagSpecifications=[{
            'ResourceType': 'instance',
            'Tags': [{
                'Key': 'Name',
                'Value': instance_name
            }]
        }]
    )

    # Get the instance ID from the response
    instance_id = response['Instances'][0]['InstanceId']

    # Return a dictionary with the instance details
    instance_info = {
        "InstanceId": instance_id,
        "InstanceName": instance_name,
        "Region": region,
        "PrivateIP": private_ip,
        "VPCID": vpc_id,
        "SubNetID": subnet_id,
        "SecurityGroupID": security_group_id  # Include the Security Group ID in the returned dictionary
    }

    waiter = ec2_client.get_waiter('instance_running')
    waiter.wait(InstanceIds=[instance_id])
    print(f"Instance {instance_name} created with Instance ID: {instance_id} and Security Group ID: {security_group_id}")

    return instance_info



def delete_instance(instance_ids, region):
    """
    Terminate an EC2 instance based on the provided instance ID and region.

    Parameters:
    instance_id (str): The ID of the instance to terminate.
    region (str): The region where the instance is located.

    Returns:
    str: The termination status of the instance.
    """
    # Initialize EC2 client for the specified region
    ec2_client = boto3.client('ec2', region_name=region)

    # Terminate the instance
    response = ec2_client.terminate_instances(InstanceIds=instance_ids)

    # Retrieve the termination status from the response
    termination_status = response['TerminatingInstances'][0]['CurrentState']['Name']

    print(f"termination status: {termination_status}.")

    return termination_status




def modify_instance_type(instances, instance_type):
    """
    Modifies the instance type for a list of EC2 instances.

    Parameters:
    instances (list): A list of dictionaries, each with 'InstanceId' and 'Region' keys.
    instance_type (str): The new instance type to apply (e.g., 't3.micro').

    Returns:
    list: A list of dictionaries with 'InstanceId', 'Region', and the result of the operation.
    """
    results = []

    for instance in instances:
        instance_id = instance['InstanceId']
        region = instance['Region']

        ec2_client = boto3.client('ec2', region_name=region)

        try:
            # Stop the instance
            ec2_client.stop_instances(InstanceIds=[instance_id])
            print(f"Stopping instance {instance_id} in region {region}...")

            # Wait for the instance to stop
            waiter = ec2_client.get_waiter('instance_stopped')
            waiter.wait(InstanceIds=[instance_id])
            print(f"Instance {instance_id} is now stopped.")

            # Modify the instance type
            ec2_client.modify_instance_attribute(
                InstanceId=instance_id,
                InstanceType={'Value': instance_type}
            )
            print(f"Modified instance {instance_id} to type {instance_type}.")

            # # Start the instance again
            # ec2_client.start_instances(InstanceIds=[instance_id])
            # print(f"Starting instance {instance_id} in region {region}...")

            # # Wait for the instance to start
            # waiter = ec2_client.get_waiter('instance_running')
            # waiter.wait(InstanceIds=[instance_id])
            # print(f"Instance {instance_id} is now running with type {instance_type}.")

            # Add result to list
            results.append({
                'InstanceId': instance_id,
                'Region': region,
                'Status': 'Success'
            })

        except ClientError as e:
            print(f"Error modifying instance {instance_id} in region {region}: {e}")
            results.append({
                'InstanceId': instance_id,
                'Region': region,
                'Status': f"Failed - {e}"
            })

    return results


def copy_ami_to_multiple_regions(ami_id, regions):
    """
    Copies an AMI to multiple regions and returns a list with AMI details in each region.

    Parameters:
    ami_id (str): The ID of the AMI to copy.
    regions (list): A list of regions where the AMI should be copied.

    Returns:
    list: A list of dictionaries containing the AMI name, ID, and regions where it was copied.
    """
    ami_info = []

    # Initialize EC2 client for the default region (the region of the source AMI)
    ec2_client = boto3.client('ec2')

    # Get the AMI name to use for the copy
    describe_ami_response = ec2_client.describe_images(ImageIds=[ami_id])
    ami_name = describe_ami_response['Images'][0]['Name']

    for region in regions:
        # Create an EC2 client for the target region
        target_client = boto3.client('ec2', region_name=region)

        # Copy the AMI to the target region
        copy_response = target_client.copy_image(
            Name=ami_name,
            SourceImageId=ami_id,
            SourceRegion=ec2_client.meta.region_name  # The source region of the AMI
        )

        # Add the copied AMI details to the result list
        ami_info.append({
            "AMI Name": ami_name,
            "AMI ID": copy_response['ImageId'],
            "Region": region
        })

    return ami_info

