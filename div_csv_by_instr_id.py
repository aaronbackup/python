import pandas as pd
def load_instr_id():
    id_file="/Users/mac/data/ids.csv"
    df = pd.read_csv(id_file)
    return df["instr_id"]

id_list=load_instr_id()
csv_file="/Users/mac/data/price.csv"
chunksize=1000
need_head={}
for df in pd.read_csv(csv_file,chunksize=5):
    df.to_csv("/Users/mac/data/" + str("all") + ".csv",mode='a',header= True,index=None)
    for id in id_list:
        df1 = df[(df.instr_id == id) & ((df.source == "LtsUdp:2:1") | (df.source == "LtsUdp:2:2"))]
        df1["instr_ids"] = df1['instr_id'].apply(lambda x: str(x).zfill(6))
        df1.to_csv("/Users/mac/data/" + str(id).zfill(6) + ".csv",mode='a',header= id not in need_head,index=None)
        need_head[id]=False
