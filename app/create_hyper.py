#!/usr/bin/env python
# coding: utf-8

# In[54]:


import pandas as pd
from pymongo import MongoClient
import datetime, time
import pantab
import time
import tableauserverclient as TSC
from tableau_tools import *
from tableau_tools.tableau_documents import *
import os
from tableauhyperapi import HyperProcess, Connection, TableDefinition, SqlType, Telemetry, Inserter, CreateMode, TableName
from tableauhyperapi import escape_string_literal
tableau_auth = TSC.TableauAuth('Admin', 'Boller555!')
server = TSC.Server('http://35.206.224.237/')
#server = TSC.Server('http://localhost/')
project_id = '9cb8352b-7440-40ed-8956-a7f5211c5e2c'


client=MongoClient(host='192.168.4.200', port=27017,username="root", password="Boller555!")
#client=MongoClient('mongodb://dbreader:Ab12345@mongodb01.bollergame.local:27017/?replicaSet=rs0&readPreference=secondary')
tdsx_list=['gift.tdsx',
 'item.tdsx',
 'login_game.tdsx',
 'login_point_member.tdsx',
 'login_purchase.tdsx',
 'member.tdsx',
 'purchase_analysis.tdsx',
 'purchase_rank.tdsx',
 'transfer.tdsx']


# In[55]:


def login():
    db = client['LogDB']['Log_LoginRecord']
    pipeline = [{'$match':{"logintime":{"$gte": datetime.datetime(2021,5,1)}}},{'$project':{'_id':0,'accountid':1,'logintime':1,'logouttime':1,'channel':1,'vip':1}}]
    print('db connected')
    data = pd.DataFrame(list(db.aggregate(pipeline)))
    if len(data)==0:
        return
    print('data downloaded')
    df=pd.DataFrame(
    [[aid,time,channel,vip] for aid,lt1,lt2,channel,vip in data.itertuples(index=False)
     for time in pd.date_range(lt1.floor('h'),(lt2+datetime.timedelta(seconds=30)).floor('h'),freq='h')],
     columns=['accountid','time','channel','vip'])
    df=df.sort_values('vip', ascending=False).drop_duplicates(['accountid','time'])
    df.channel=df.channel.fillna("")
    df.channel=df.channel.replace('AppleStore','AppStore')
    df.channel=df.channel.replace('','Web')
    df.vip=df.vip.fillna(-1)
    df.vip=df.vip.astype('int')
    df.accountid=df.accountid.astype('str')
    print('data proccessed')
    df.to_csv('login.csv',index=False)
    print('data saved')
    return 
def lastlogin():
    df=pd.read_csv('login.csv')
    df=df.sort_values('time').drop_duplicates('accountid',keep='last')
    df.to_csv('lastlogin.csv',index=False)
    return


# In[56]:


def purchase():
    col = client['LogDB']['IAPLog']
    list_tmp = []
    for i in col.find({},{'_id':1,'accountid':1,'amount':1,'itempackageid':1,'createtime':1,'paymethod':1,'paytype':1,'platform':1,'point':1,'successtime':1}):
        list_tmp.append(i)
    if len(list_tmp)==0:
        return
    df = pd.DataFrame(list_tmp)
    col = client['MemberDB']['ItemPackage']
    list_tmp = []
    for i in col.find({},{'_id':1,'name':1}):
        list_tmp.append(i)
    df2 = pd.DataFrame(list_tmp)
    df.itempackageid.replace(df2.set_index('_id')['name'],inplace=True)
    df.paytype.fillna("unknown",inplace=True)
    df.paytype.replace('','unknown',inplace=True)
    df.paymethod.fillna("unknown",inplace=True)
    df.paymethod.replace('','unknown',inplace=True)
    df.successtime=pd.to_datetime(df.successtime, errors = 'coerce')
    df.accountid=df.accountid.astype('str')
    df._id=df._id.astype('str')
    df.to_csv('purchase.csv',index=False)
    pd.DataFrame({'platform':df.platform.unique()}).to_csv('platform.csv',index=False)
    pd.DataFrame({'itempackageid':df.itempackageid.unique()}).to_csv('itempackageid.csv',index=False)
    return df


# In[57]:


def point():
    source= {0: '信件',
  1: '商店',
  2: '商城禮包',
  3: '任務',
  7: '活動',
  8: '修改暱稱',
  9: '道具合成',
  12: '使用寶箱',
  13: '開運金',
  14: '登入活動',
  15: '元素碎片',
  16: '新手教學',
  17: '排行榜活動',
  18: '手機綁定',
  19: '虛寶卡',
  20: '活動道具合成',
  21: '升級卡片',
  22: '序號兌換',
  901: '後台帳務操作',
  902: '後台信件',
  1001: 'GM',
  1002: 'GM',
  1101: '遊戲押注',
  1102: '遊戲贏分',
  1103: '贈禮成立',
  1104: '贈禮成功收取',
  1105: '贈禮取消退回'}
    col = client['LogDB']['ItemLog']
    list_tmp = []
    for i in col.find({'item':{'$in':['1','2']}},{'_id':1,'accountid':1,'time':1,'source':1,'count':1}):
        list_tmp.append(i)
    if len(list_tmp)==0:
        return
    df1 = pd.DataFrame(list_tmp)
    df1.replace({"source": source},inplace=True)
    df1['count']=df1['count'].astype(str).astype(float)
    col = client['GameRecord']['GameRecordGroup']
    list_tmp = []
    for i in col.find({},{'_id':1,'accountid':1,'dayhour':1,'JP':1,'bet':1,'win':1}):
        list_tmp.append(i)
    if len(list_tmp)==0:
        return
    df2 = pd.DataFrame(list_tmp)
    df2.dayhour=pd.to_datetime(df2.dayhour, format='%Y%m%d%H')
    df2=df2.rename(columns={'dayhour':'time','win':'贏分','bet':'押注','JP':'彩金'})
    df2[['贏分','押注','彩金']]=df2[['贏分','押注','彩金']].astype(str).astype(float)
    df2.押注=-df2.押注
    df2=df2.melt(id_vars=['_id','accountid','time'],var_name='source',value_name='count')
    df2=df2[df2['count']!=0.0]
    df=pd.concat([df1,df2],ignore_index=True)
    df.accountid=df.accountid.astype(str)
    df.source=df.source.astype(str)
    df._id=df._id.astype(str)
    df.to_csv('point.csv',index=False)
    pd.DataFrame({'source':df.source.unique()}).to_csv('source.csv',index=False)
    return


# In[58]:


def member():
    col = client['MemberDB']['AccountInfo']
    list_tmp = []
    for i in col.find({},{'_id':1,'memberid':1,'nickname':1,'createtime':1,'lastlogintime':1}):
        list_tmp.append(i)
    if len(list_tmp)==0:
        return
    df=pd.DataFrame(list_tmp)
    df['_id']=df['_id'].astype('str')
    col = client['MemberDB']['MemberInformation']
    list_tmp = []
    for i in col.find({},{'_id':1,'name':1,'phone':1,'email':1}):
        list_tmp.append(i)
    df2=pd.DataFrame(list_tmp)
    df2['_id']=df2['_id'].astype('str')
    col = client['MemberDB']['VIPLevel']
    list_tmp = []
    for i in col.find({},{'_id':1,'viplevel':1}):
        list_tmp.append(i)
    df3=pd.DataFrame(list_tmp)
    df3['_id']=df3['_id'].astype('str')
    col = client['MemberDB']['CreateAccountInfo']
    list_tmp = []
    for i in col.find({},{'_id':1,'channel':1,'type':1}):
        list_tmp.append(i)
    df4 = pd.DataFrame(list_tmp)
    df4['_id']=df4['_id'].astype('str')
    df=df.merge(df2,how='left',on='_id')
    df=df.merge(df3,how='left',on='_id')
    df=df.merge(df4,how='left',on='_id')
    df.channel=df.channel.replace('AppleStore','AppStore')
    df.channel=df.channel.fillna("unknown")
    df.channel=df.channel.replace('','unknown')
    df.type=df.type.fillna("unknown")
    df.type=df.type.replace('','unknown')
    df[['phone','name','email']]=df[['phone','name','email']].fillna('')
    df.viplevel.fillna(0,inplace=True)
    df.viplevel=df.viplevel.astype('int')
    df.phone=df.phone.astype(str)
    df.lastlogintime=pd.to_datetime(df.lastlogintime, errors = 'coerce')
    df.rename(columns={'channel':'reg_channel','type':'reg_type'},inplace=True)
    df.to_csv('member.csv',index=False)
    pd.DataFrame({'reg_type':df.reg_type.unique()}).to_csv('reg_type.csv',index=False)
    pd.DataFrame({'reg_channel':df.reg_channel.unique()}).to_csv('reg_channel.csv',index=False)
    return


# In[60]:


def itemlog():
    col = client['LogDB']['ItemLog']
    list_tmp = []
    for i in col.find({'item':{'$nin':['1','2']}},{'_id':1,'accountid':1,'time':1,'source':1,'count':1,'item':1}):
        list_tmp.append(i)
    if len(list_tmp)==0:
        return
    df = pd.DataFrame(list_tmp)
    
    df['count']=df['count'].astype(str).astype(int)
    df._id=df._id.astype(str)
    df.accountid=df.accountid.astype(str)
    df.item=df.item.astype(int)
    df.to_csv('itemlog.csv',index=False)
    return


# In[71]:


def game():
    gameid_dict={1: '牛魔王', 2: '牛魔王', 3: '牛魔王',
 5: '掏金樂', 6: '掏金樂', 7: '掏金樂',
 9: '武財神', 10: '武財神', 11: '武財神',
 13: '三太子', 14: '三太子', 15: '三太子',
 17: '悟空', 18: '悟空', 19: '悟空',
 21: '鐵扇', 22: '鐵扇', 23: '鐵扇',
 25: '八戒', 26: '八戒', 27: '八戒',
 29: '呂布', 30: '呂布', 31: '呂布',
 33: '周瑜', 34: '周瑜', 35: '周瑜',
 37: '曹操', 38: '曹操', 39: '曹操',
 41: '趙雲', 42: '趙雲', 43: '趙雲',
 44: '法老王', 45: '法老王', 46: '法老王'}
    groupid_dict={0:'娛樂廳',  1:'富貴廳',   2:'帝王廳'}
    col = client['GameRecord']['GameRecordGroup']
    list_tmp = []
    for i in col.find({},{'_id':0, 'accountid':1, 'dayhour':1, 'gameid':1, 'gametype':1, 'groupid':1, 'seat':1,
           'JP':1, 'bet':1, 'win':1, 'useitem':1,'count':1}):
        list_tmp.append(i)
    if len(list_tmp)==0:
        return
    df = pd.DataFrame(list_tmp)
    df.dayhour=pd.to_datetime(df.dayhour,format='%Y%m%d%H')
    df.accountid=df.accountid.astype('str')
    df.useitem=df.useitem.astype(str)
    df=df.rename(columns={'dayhour':'time_play'})
    df.gameid=df.gameid.astype('int')
    df[['JP','bet','win']]=df[['JP','bet','win']].astype(str).astype('float')
    df.replace({"gameid": gameid_dict},inplace=True)
    df.replace({"groupid": groupid_dict},inplace=True)
    df['lasttime']=df.groupby(by=['accountid','gameid'])['time_play'].shift()
    df['lasttime']=pd.to_datetime(df.lasttime, errors = 'coerce')
    df['count'].fillna(0,inplace=True)
    df['count']=df['count'].astype('int')
    df.to_csv('game.csv',index=False)
    pd.DataFrame({'groupid':list(set(groupid_dict.values()))}).to_csv('groupid.csv',index=False)
    pd.DataFrame({'gameid':list(set(gameid_dict.values()))}).to_csv('gameid.csv',index=False)
    return 


# In[61]:


def id_list():
    df_idlist=pd.DataFrame({'accountid':['60a323ddafcfd1df52d350ea','5eb21433c6beb35764458c01', '5ff4354774edc59009cb3e61',
       '60a323ddafcfd1df52d350ea']})
    df_idlist.to_csv('id_list.csv',index=False)
    return


# In[62]:


def gift():
    db = client['MemberDB']['GiftTransferTable']
    pipeline = [{"$unwind": "$items"} ,{'$project':{'_id':1,'transferid':1,'createtime':1,'transferaccountid':1,"receivemember":1,'status':1,'lastmodifydate':1,'items':1}}]
    df=pd.DataFrame(list(db.aggregate(pipeline)))
    if len(df)==0:
        return
    df=df.join(pd.concat(pd.DataFrame.from_dict(i,orient='index') for i in df.pop('items')).reset_index())
    df.rename(columns={'index':'itemid',0:'count'},inplace=True)
    df._id=df._id.astype(str)
    df.itemid=df.itemid.astype(int)
    df.transferaccountid=df.transferaccountid.astype(str)
    df.receivemember=df.receivemember.astype(str)
    df=df.merge(pd.read_csv('member.csv',low_memory=False)[['_id','memberid','nickname','viplevel']].rename(columns={'_id':'transferaccountid'}),on='transferaccountid').rename(columns={'memberid':'tran_memberid','nickname':'tran_nickname','viplevel':'tran_vip'})
    df=df.merge(pd.read_csv('member.csv',low_memory=False)[['_id','memberid','nickname','viplevel']].rename(columns={'_id':'receivemember'}),on='receivemember').rename(columns={'memberid':'rece_memberid','nickname':'rece_nickname','viplevel':'rece_vip'})
    df=df.merge(pd.read_csv('lastlogin.csv')[['accountid','channel']].rename(columns={'accountid':'transferaccountid'}),on='transferaccountid').rename(columns={'channel':'tran_channel'})
    df=df.merge(pd.read_csv('lastlogin.csv')[['accountid','channel']].rename(columns={'accountid':'receivemember'}),on='receivemember').rename(columns={'channel':'rece_channel'})
    df.loc[df['transferaccountid'].isin(pd.read_csv('id_list.csv')['accountid']),'tran_inlist']=1
    df.loc[df['receivemember'].isin(pd.read_csv('id_list.csv')['accountid']),'rece_inlist']=1
    df.rece_inlist.fillna(0,inplace=True)
    df.tran_inlist.fillna(0,inplace=True)
    df[['tran_inlist','rece_inlist']]=df[['tran_inlist','rece_inlist']].astype(int)
    df.to_csv('gift.csv',index=False)
    return


# In[63]:


def trans():
    db = client['MemberDB']['GiftTransferTable']
    df=pd.DataFrame(list(db.find({},{'_id':1,'transferid':1,'createtime':1,'transferaccountid':1,"receivemember":1,"transferpoint":1,"fee":1,'status':1,'lastmodifydate':1})))
    if len(df)==0:
        return    
    df._id=df._id.astype(str)
    df.transferaccountid=df.transferaccountid.astype(str)
    df.receivemember=df.receivemember.astype(str)
    df1=df[['_id', 'transferid', 'createtime', 'transferaccountid', 'transferpoint', 'fee','status', 'lastmodifydate']].rename(columns={'transferaccountid':'accountid'})
    df1['type']='transfer'
    df2=df[['_id', 'transferid', 'createtime', 'receivemember', 'transferpoint', 'fee','status', 'lastmodifydate']].rename(columns={'receivemember':'accountid'})
    df2['type']='receive'
    df=pd.concat([df1,df2], ignore_index=True)
    df=df.merge(pd.read_csv('member.csv',low_memory=False)[['_id','memberid','nickname','viplevel']].rename(columns={'_id':'accountid'}),on='accountid')
    df=df.merge(pd.read_csv('lastlogin.csv',low_memory=False)[['accountid','channel']],on='accountid')
    df[['fee','transferpoint']]=df[['fee','transferpoint']].astype(str).astype(float)
    df.loc[df['accountid'].isin(pd.read_csv('id_list.csv')['accountid']),'inlist']=1
    df.inlist.fillna(0,inplace=True)
    df.inlist=df.inlist.astype(int)
    df.to_csv('trans.csv',index=False)
    pd.DataFrame({'status':df.status.unique()}).to_csv('status.csv',index=False)
    return


# In[64]:


def table():
    timetable=pd.DataFrame({'time':pd.date_range(start='2021-04-01',end=datetime.date.today()+datetime.timedelta(days=1),freq='h')})
    vip=pd.DataFrame({'vip':[-1,0,1,2,3,4,5,6]}) 
    channel=pd.DataFrame({'channel':['AppStore','GooglePlay','Web']})
    #source=pd.DataFrame({'source':pd.read_csv('purchase.csv').source.unique()})
    #reg_type=pd.DataFrame({'reg_type':pd.read_csv('member.csv').reg_type.unique()})
    #reg_channel=pd.DataFrame({'reg_channel':pd.read_csv('member.csv').reg_channel.unique()})
    #groupid=pd.DataFrame({'groupid':list(set(groupid_dict.values()))})
    #gameid=pd.DataFrame({'gameid':list(set(gameid_dict.values()))})
    #item=pd.read_excel('item.xlsx')
    #platform=pd.DataFrame({'platform':df_purchase.platform.unique()})
    #itempackageid=pd.DataFrame({'itempackageid':df_purchase.itempackageid.unique()})
    vip.to_csv('vip.csv',index=False)
    channel.to_csv('channel.csv',index=False)
    timetable.to_csv('timetable.csv',index=False)
    return


# In[65]:


def csv_to_hyper(hyper):
    dict_df={}
    csv_list=[]
    if hyper=="login_purchase.hyper":
        csv_list=['login','purchase','timetable','vip','channel','id_list']
    elif hyper=="login_point_member.hyper":
        csv_list=['login','point','timetable','vip','channel','source','member'] 
    elif hyper=="member.hyper":
        csv_list=['member','timetable','reg_channel','reg_type']
    elif hyper=="login_game.hyper":
        csv_list=['login','game','timetable','vip','channel','gameid','groupid'] 
    elif hyper=="purchase_rank.hyper":
        csv_list=['login','game','timetable','vip','channel','member','purchase']      
    elif hyper=="item.hyper":
        csv_list=['timetable','item','itemlog']
    elif hyper=="purchase_analysis.hyper":
        csv_list=['purchase','timetable','itempackageid','platform']            
    elif hyper=="transfer.hyper":
        csv_list=['trans','timetable','vip','channel','status']
    elif hyper=="gift.hyper":
        csv_list=['gift','timetable','vip','item','status']         
    for i in csv_list:
        dict_df[i]=pd.read_csv(f'{i}.csv',low_memory=False,nrows=1)
        dict_df[i]=pd.read_csv(f'{i}.csv',low_memory=False,parse_dates=[l for l in dict_df[i].columns if l.find('time')>-1],infer_datetime_format=True)    
    pantab.frames_to_hyper(dict_df, hyper)     
    return
def swap_hyper(hyper_name, tdsx_name, logger_obj=None):
    
    local_tds = TableauFileManager.open(filename=tdsx_name, logger_obj=logger_obj)

    filenames = local_tds.get_filenames_in_package()
    for filename in filenames:
        if filename.find('.hyper') != -1:
            print("Overwritting Hyper in original TDSX...")
            local_tds.set_file_for_replacement(filename_in_package=filename,
                                            replacement_filname_on_disk=hyper_name)
            break
    
    tdsx_name_before_extension, tdsx_name_extension = os.path.splitext(tdsx_name)
    tdsx_updated_name = tdsx_name_before_extension + '_updated' + tdsx_name_extension
    local_tds.save_new_file(new_filename_no_extension=tdsx_updated_name)
    os.remove(tdsx_name)
    os.rename(tdsx_updated_name, tdsx_name)


# In[66]:


if __name__ == '__main__':
    t1=time.time()
    print('start creating csv')
    id_list()
    t2=time.time()
    print(f'{t2-t1}:id_list.csv created')
    purchase()
    t3=time.time()
    print(f'{t3-t1}:purchase.csv created')
    login()
    t4=time.time()
    print(f'{t4-t1}:login.csv created')
    lastlogin()
    member()
    t5=time.time()
    print(f'{t5-t1}:member.csv created')   
    point()
    print('point.csv created') 
    game()
    t6=time.time()
    print(f'{t6-t1}:game.csv created')
    itemlog()
    t7=time.time()
    print(f'{t7-t1}:itemlog.csv created')
    gift()
    trans()
    print('trans.csv created')
    table()
    for i in ['gift.hyper',  'item.hyper', 'login_game.hyper', 'login_point_member.hyper', 'login_purchase.hyper', 'member.hyper', 'purchase_analysis.hyper', 'purchase_rank.hyper', 'transfer.hyper']:
        csv_to_hyper(i)
        print (f'{i} is finished')
    print (pd.read_csv('login.csv').shape)
    for i in tdsx_list:
        swap_hyper(i.replace('tdsx','hyper'),i)    
    with server.auth.sign_in(tableau_auth):
        for i in tdsx_list:
            new_datasource = TSC.DatasourceItem(project_id)
            new_datasource = server.datasources.publish(new_datasource, i, 'Overwrite')
        all_datasources, pagination_item = server.datasources.get()
        print("\nThere are {} datasources on site: ".format(pagination_item.total_available))
        print([datasource.name for datasource in all_datasources])


# In[ ]:




