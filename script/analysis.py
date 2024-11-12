import pandas as pd
from IPython import embed;
import argparse
import datetime
import os

LOGIN_PATH = "/home/ubuntu"



def throughput_apply_func(group):
    if len(group):
        return pd.Series({
            'AvgThroughput':len(group),
        })

def ThroughputAnalysis(merge_df):
    merge_df.loc[:, "time"] = merge_df['CommitTime'].apply(
                lambda us_ts: datetime.datetime.fromtimestamp(us_ts * 1e-6))
    bin_interval_s = 1
    grouped = merge_df.groupby(
        pd.Grouper(key='time', freq='{}s'.format(bin_interval_s)))
    grouped_apply_orders = grouped.apply(throughput_apply_func)
    grouped_apply_orders = grouped_apply_orders.dropna()
    grouped_apply_orders = grouped_apply_orders[5:-5]
    # print(grouped_apply_orders['AvgThroughput'])
    throughput = (grouped_apply_orders['AvgThroughput']/bin_interval_s).mean()
    return throughput


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--num_clients',  type=int, default = 10,
                        help='Specify the number of clients ')
    parser.add_argument('--stats_path',  type=str, default = "",
                        help='Specify path of stats')
    args = parser.parse_args()

    num_clients = args.num_clients
    print("clients: ", num_clients)


    stats_folder = "{login_path}/{folder_name}".format(
        login_path = LOGIN_PATH,
        folder_name = args.stats_path
    )

    client_df_list = []
    for i in range(num_clients):
        file_name = "Client-"+str(i)
        if os.path.exists(stats_folder+"/"+file_name) is False:
            print(f"{file_name} not exists in {stats_folder}")
            continue
        client_df = pd.read_csv(stats_folder+"/"+file_name)
        client_df_list.append(client_df)
    client_df = pd.concat(client_df_list)


    stats = ""
    stats += "Num:"+str(len(client_df))+"\n"
    stats += "50p:\t"+str(client_df['Latency'].quantile(.5))+"\n"
    stats += "75p:\t"+str(client_df['Latency'].quantile(.75))+"\n"
    stats += "90p:\t"+str(client_df['Latency'].quantile(.9))+"\n"
    print(stats)

    throughput_stats = ThroughputAnalysis(client_df)
    print("Throughput ", throughput_stats)


    # embed()