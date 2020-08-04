import os,sys
import math
import random
import time 

# 构建全局容器
ok = {} # ok['h'+'t'][r] = 1 
a ={} # a[h][r][t] = 1 # 用于存储dataset三元组

relation2id = {}
id2relation = {}
relation_num = 0

h_e_p = {} # h_e_p['e1'+'e2'][rel_path] = 可信度(没进行归一化)
path_dict = {} # path 2 mount  即统计每个path的数量 作用是统计path对应的频数
path_r_dict = {} # path_r_dict[rel_path+"->" rel] = 数量 ,作用是统计path和rel的共现频数


def map_add(mp, key1,key2, value):
    '''
    容器操作: mp[key1][key2] += value
    '''
    if (key1 not in mp):
        mp[key1] = {}
    if (key2 not in mp[key1]):
        mp[key1][key2] = 0.0
    mp[key1][key2] += value


def map_add1(mp,key):
    '''
    容器操作: mp[key]+=1
    '''
    if (key not in mp):
        mp[key] = 0
    mp[key]+=1


def load_rel2id(file):
    '''
    load relation2id file
    '''
    global relation2id
    global id2relation
    global relation_num

    # load relation2id
    f = open(file,"r")
    for line in f:
        rel_id, rel_str = line.strip().split('\t')
        relation2id[rel_str] = int(rel_id)
        id2relation[int(rel_id)] = rel_str
        id2relation[int(rel_id)+ relation_num] = "~" + rel_id
        relation_num += 1
    f.close()


def load_triples(file):
    '''
    load triples
    输入格式(h, r, t)
    '''
    global ok
    global a

    # load dataset
    f = open(file,"r")
    for line in f:
        seg = line.strip().split('\t')
        e1 = seg[0]
        rel = seg[1] # string
        e2 = seg[2]
        # 加入ok
        if (e1+" "+e2 not in ok):
            ok[e1+" "+e2] = {}
        ok[e1+" "+e2][relation2id[rel]] = 1 # e1 e2加入正向边
        if (e2+" "+e1 not in ok):
            ok[e2+" "+e1] = {}
        ok[e2+" "+e1][relation2id[rel]+relation_num] = 1 # e1 e2加入反向边
        # 加入a
        if (e1 not in a):
            a[e1]={}
        if (relation2id[rel] not in a[e1]):
            a[e1][relation2id[rel]]={}
        a[e1][relation2id[rel]][e2]=1 # 加入正向边
        if (e2 not in a):
            a[e2]={}
        if ((relation2id[rel]+relation_num) not in a[e2]):
            a[e2][relation2id[rel]+relation_num]={}
        a[e2][relation2id[rel]+relation_num][e1]=1 # 加入反向边
    f.close()


def generate_path():
    '''
    search path for triple
    '''
    global h_e_p
    global path_dict
    global path_r_dict
    print('[INFO] 开始进行路径搜索')
    step = 0 # 进度统计
    for e1 in a:
        # 打印进度
        step += 1
        if step % 1000 == 0:
            print('process: %.4lf' % (step / len(a)))
        # 处理one-hop path:(e1, rel1, e2)
        for rel1 in a[e1]:
            e2_set = a[e1][rel1]
            for e2 in e2_set:
                map_add1(path_dict, str(rel1)) # e1和e2之间的一跳路径，作用是统计每条路径的出现次数
                for key in ok[e1+' '+e2]:
                    map_add1(path_r_dict, str(rel1) + "->" + str(key))
                map_add(h_e_p, e1+' '+e2, str(rel1), 1.0 / len(e2_set))
        # 处理two-hop path:(e1, rel1, e2)的e2基础上再拓展一层节点e3
        for rel1 in a[e1]:
            e2_set = a[e1][rel1]
            for e2 in e2_set:
                if (e2 in a):
                    for rel2 in a[e2]:
                        e3_set = a[e2][rel2]
                        for e3 in e3_set:
                            map_add1(path_dict,str(rel1)+" "+str(rel2)) # 将这个两跳的路径加入，统计出现次数
                            if (e1+" "+e3 in ok):
                                for key in ok[e1+' '+e3]:
                                    map_add1(path_r_dict,str(rel1)+" "+str(rel2)+"->"+str(key)) 
                            if (e1+" "+e3 in ok):# and h_e_p[e1+' '+e2][str(rel1)]*1.0/len(e3_set)>0.01):
                                map_add(h_e_p, e1+' '+e3, str(rel1)+' '+str(rel2),h_e_p[e1+' '+e2][str(rel1)]*1.0/len(e3_set))


def proir_path_confidence(in_file, out_file):
    '''
    triple with prior path confidence
    '''
    f = open(in_file, "r")
    g = open(out_file, "w")
    for line in f:
        seg = line.strip().split('\t')
        e1 = seg[0]
        rel = relation2id[seg[1]]
        e2 = seg[2]
        # 计算正向triple路径
        # g.write(str(e1) + ' ' +str(e2) + ' ' + str(rel) + ' ') # triple
        b = {}
        a = {}
        # 计算每条path的可信度
        res = 0 # prior path confidence
        if (e1+' '+e2 in h_e_p):
            # 可信度的归一化预处理
            sum = 0.0
            for rel_path in h_e_p[e1+' '+e2]:
                b[rel_path] = h_e_p[e1+' '+e2][rel_path]
                sum += b[rel_path]
            for rel_path in b:
                b[rel_path]/=sum
                if b[rel_path]>0.01:
                    a[rel_path] = b[rel_path] # a中存的就是每条路径的可信度rel_path to score
            # 累加计算
            for rel_path in a:
                if (rel_path in path_dict and rel_path+"->"+str(rel) in path_r_dict):
                    q = path_r_dict[rel_path+"->"+str(rel)] * 1.0 / path_dict[rel_path]
                    r = a[rel_path]
                    # res += q * r
                    res += (0.2 + 0.8 * q) * r
        g.write(str(res) + '\n') 
    f.close()
    g.close()    


def work(in_file, out_file):
    '''
    triple and path with confidence
    '''
    f = open(in_file, "r")
    g = open(out_file, "w")
    for line in f:
        seg = line.strip().split('\t')
        e1 = seg[0]
        rel = relation2id[seg[1]]
        e2 = seg[2]
        # 计算正向triple路径
        g.write(str(e1)+" "+str(e2)+' '+str(rel)+"\n") # triple
        b = {}
        a = {}
        if (e1+' '+e2 in h_e_p):
            sum = 0.0
            # 计算总分，为了进行归一化
            for rel_path in h_e_p[e1+' '+e2]:
                b[rel_path] = h_e_p[e1+' '+e2][rel_path]
                sum += b[rel_path]
            # 对每条路径score进行归一化
            for rel_path in b:
                b[rel_path]/=sum
                if b[rel_path]>0.01:
                    a[rel_path] = b[rel_path] # a中存的就是每条路径的可信度rel_path to score
        g.write(str(len(a))) # 路径数量
        for rel_path in a:
            g.write(" "+str(len(rel_path.split()))+" "+rel_path+" "+str(a[rel_path])) # 每条路径信息
        g.write("\n")
        # 计算反向triple路径
        g.write(str(e2)+" "+str(e1)+' '+str(rel+relation_num)+"\n")
        e1 = seg[1]
        e2 = seg[0]
        b = {}
        a = {}
        if (e1+' '+e2 in h_e_p):
            sum = 0.0
            for rel_path in h_e_p[e1+' '+e2]:
                b[rel_path] = h_e_p[e1+' '+e2][rel_path]
                sum += b[rel_path]
            for rel_path in b:
                b[rel_path]/=sum
                if b[rel_path]>0.01:
                    a[rel_path] = b[rel_path]
        g.write(str(len(a)))
        for rel_path in a:
            g.write(" "+str(len(rel_path.split()))+" "+rel_path+" "+str(a[rel_path]))
        g.write("\n")
    f.close()
    g.close()


if __name__ == '__main__':
    print('start')
    data_path = './NL27K_with_neg/'
    load_rel2id(data_path + 'relation2id.tsv')
    load_triples(data_path + 'pos_with_neg.tsv')
    print('[INFO] 数据载入完成')
    generate_path()
    proir_path_confidence(data_path + "pos_with_neg.tsv", data_path + "pos_with_neg_PP_conf.txt") # file_confidence
    work(data_path + "pos_with_neg.tsv", data_path + 'pos_with_neg_pra.txt') # file_pra.txt