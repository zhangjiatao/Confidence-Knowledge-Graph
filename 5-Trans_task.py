'''
from base 2 tasks
'''
import random
import json
from collections import defaultdict

in_path = './NL27K_with_neg_res/'
out_path = './NL27K-tasks/'

low_freq = 50
up_freq = 500

ent2id = None
id2ent = None
rel2id = None
id2rel = None
rel2candidates = None
rels_id_train = None
rels_id_val = None
rels_id_test = None

'''=========================== 基础功能函数 =========================== '''

def read_symbol2id(file):
    '''
    read ent2id or relation2id file
    ps 这里都是针对NL27K-base数据集中格式的读取
    '''
    symbol2id = {}
    id2symbol = {}
    with open(file) as f:
        lines = f.readlines()
        for line in lines:
            tmp = line.replace('\n', '').split('\t')
            symbol_id, symbol_str = tmp[0], tmp[1]
            symbol2id[symbol_str] = symbol_id
            id2symbol[symbol_id] = symbol_str

    return symbol2id, id2symbol

def read_triples_id(file):
    '''
    read triple file
    '''
    triples = []
    with open(file) as f:
        lines = f.readlines()
        for line in lines:
            tmp = line.replace('\n', '').split('\t')
            triples.append((tmp[0], tmp[1], tmp[2], tmp[3])) # (h, r, t, s),
    f.close()   
    return triples

def read_triples_str(file):
    '''
    read triple file
    input: file id format
    output: string format
    '''
    triples = []
    with open(file) as f:
        lines = f.readlines()
        for line in lines:
            (h, r, t, s) = line.replace('\n', '').split('\t')
            triples.append((id2ent[h], id2rel[r], id2ent[t], s)) # (h, r, t, s),
    f.close()   
    return triples


def write_triples(file, triples):
    '''
    write triples file
    '''
    with open(file, 'w') as f:
        for triple in triples:
            line = str(triple[0]) + '\t' + str(triple[1]) + '\t' + str(triple[2]) + '\t' + str(triple[3]) + '\n'
            f.write(line)
    f.close()


def count_freq(triples):
    '''
    统计每个关系在triples中出现的频率
    '''
    ent2cnt = {}
    rel2cnt = {}

    for triple in triples:
        (h, r, t, s) = triple
        if h not in ent2cnt.keys():
            ent2cnt[h] = 0
        if r not in rel2cnt.keys():
            rel2cnt[r] = 0  
        if t not in ent2cnt.keys():
            ent2cnt[t] = 0 
        ent2cnt[h] += 1
        rel2cnt[r] += 1
        ent2cnt[t] += 1  
    
    return ent2cnt, rel2cnt

def rel_2_triple(type_triple = 'id', file_id = ''):
    '''
    构建rel2triples，用rel_id索引所有相关triples
    用于构建tasks
    '''
    rel2triples = {}
    triples = None
    if type_triple == 'id':
        triples = read_triples_id(file_id)
    else:
        triples = read_triples_str(file_id)

    for triple in triples:
        rel = triple[1]
        if rel not in rel2triples.keys():
            rel2triples[rel] = []
        rel2triples[rel].append(triple)
    return rel2triples
    

def filter_relation_freq(file_id):
    '''
    统计triples中relation的频率，通保留在目标频率区间中的relation
    ps:50-500范围内有134个关系
    '''

    triples_id = read_triples_id(file_id)
    ent2cnt, rel2cnt = count_freq(triples_id)
    tmp_list = sorted(rel2cnt.items(), key = lambda x:x[1], reverse = False)
    rel_list = []
    for (rel_id, freq) in tmp_list:
        if freq >= low_freq and freq <= up_freq:
            rel_list.append(rel_id)
    
    return rel_list

def candidate_triples(pos_triples_id):
    '''
    生成每个relation的candidate set
    '''
    all_entities = ent2id.keys()

    type2ents = defaultdict(set)
    for ent in all_entities:
        type_ = ent.split(':')[1]
        type2ents[type_].add(ent)
    pos_triples_str = [(id2ent[h], id2rel[r], id2ent[t]) for (h, r, t, s) in pos_triples_id]
    rel2triples = rel_2_triples(pos_triples_str)

    rel2candidates = {}
    for rel, triples in rel2triples.items():
        possible_types = set()
        for example in triples:
            type_ = example[2].split(':')[1] # type of tail entity
            possible_types.add(type_)
        candidates = []
        for type_ in possible_types:
            candidates += list(type2ents[type_])
        candidates = list(set(candidates))
        rel2candidates[rel] = candidates

    return rel2candidates

def rel_2_triples(pos_triples):
    '''
    构建rel2triples，用rel_id索引所有相关triples
    用于构建tasks
    '''
    rel2triples = {}
    triples = pos_triples

    for triple in triples:
        rel = triple[1]
        if rel not in rel2triples.keys():
            rel2triples[rel] = []
        rel2triples[rel].append(triple)
    return rel2triples

'''=========================== 复杂的功能函数 =========================== '''

def triples_id_2_triples_str(file_id, file_str):
    '''
    input: data_recorrect_score.tsv
    output: data_str.tsv
    并进行基本的统计
    '''
    triples_str = read_triples_str(file_id)
    write_triples(file_str, triples_str)

def triples_2_str_set(triples):
    '''
    将一个triple_list中的triple都搞成一个string并且丢到一个set中
    '''
    triple_set = set()
    for triple in triples:
        (h, r, t, s) = triple
        tmp_str = h + ',' + r + ',' + t
        triple_set.add(tmp_str)
    return triple_set

def generate_neg_examples(pos_triples_str, neg_num):
    '''
    利用正例生成负例，修改方案，只需要对于不同的r来定制不同的candidate set就ok
    '''
    neg_triples = []
    neg_set =  set()

    pos_indexs = random.sample(range(len(pos_triples_str)), neg_num) # 随机采样neg_num个正例，后面需要对这些正例打乱
    # print('随机数产生完成')
    for i, pos_index in enumerate(pos_indexs):
        (h, r, t, s) = pos_triples_str[pos_index]
        while True:
            candidates = rel2candidates[r]
            # print(id2rel[r], len(candidates))
            noise = random.choice(candidates)
            # noise = ent2id[noise]
            key_str = h + ',' + r + ',' + noise
            if  key_str not in neg_set and noise != t:
                s = random.uniform(0.0, 0.3)
                neg_triples.append((h, r, noise, s))
                neg_set.add(key_str)
                break

    # print('[INFO] 生成负例数量', len(neg_triples))
    return neg_triples

def create_tasks(file_id, out_path):
    '''
    input file: 
    output file：
    train_tasks_id.json, train_tasks_str.json(val test同样)
    path_graph_id, path_graph_str
    '''
    print('--------------Data: %s ------------' % out_path)
    rel2triples_str = rel_2_triple('str', file_id)
    rel2triples_id = rel_2_triple('id', file_id)

    def write_tasks(rels_id, desc = ''):
        # train tasks
        cnt = 0
        tasks_str = {}
        neg_triples_all = []
        neg_cnt = 0
        for rel_id in rels_id:
            pos_triples_str = rel2triples_str[id2rel[rel_id]] # 取出正例
            # ---------- 手工构造负例 ----------
            neg_num = -1
            if 'N0' in out_path:
                neg_num = 0
            elif 'N1' in out_path:
                neg_num = len(pos_triples_str) * 0.0
            elif 'N2' in out_path:
                neg_num = len(pos_triples_str) * 0.0
            elif 'N3' in out_path:
                neg_num = len(pos_triples_str) * 0.0
            else:
                print('[WARN] something error')
            neg_num = int(neg_num)
            neg_triples_str = generate_neg_examples(pos_triples_str, neg_num)
            #----------------------------------
            if desc == 'train':
                neg_triples_all = neg_triples_all + neg_triples_str
                tasks_str[id2rel[rel_id]] = pos_triples_str + neg_triples_str # 仅在训练集中加入负例
                neg_cnt += neg_num
            else:
                tasks_str[id2rel[rel_id]] = pos_triples_str
            cnt += len(rel2triples_id[rel_id])
        print(desc, 'task num', len(rels_id), cnt)

        # write
        print('[INFO] manu neg num', len(neg_triples_all), neg_cnt)
        write_triples(out_path + desc + '_neg_triples.tsv', neg_triples_all)
        with open(out_path + desc + '_tasks.json', 'w') as f:
            json.dump(tasks_str, f)
        f.close()

    write_tasks(rels_id_train, desc = 'train')
    write_tasks(rels_id_val, desc = 'dev')
    write_tasks(rels_id_test, desc = 'test')


    triples_id_all = read_triples_id(file_id)
    print('[INFO]',file_id, 'all triples num:', len(triples_id_all))
    triples_background_id = []
    triples_background_str = []
    for index, triple_id in enumerate(triples_id_all):
        (h, r, t, s) = triple_id
        if r not in rels_id_train and r not in rels_id_val and r not in rels_id_test:
            triples_background_id.append((h, r, t, s))
            triples_background_str.append((id2ent[h], id2rel[r], id2ent[t], s))
    
    print('background triples', len(triples_background_id))

    write_triples(out_path + 'path_graph', triples_background_str)



    # # 构造graph_path，这里明确了，graph path是background knowledge，是不包含train、valid、test task中的triples的,其实这里大可不必搞的这么复杂，只要不是关于train、test、dev的triple都要放进
    # triples_used = set()
    # for rel_id in rels_id_train:
    #     triples_used = triples_used | triples_2_str_set(rel2triples_id[rel_id])
    # for rel_id in rels_id_val:
    #     triples_used = triples_used | triples_2_str_set(rel2triples_id[rel_id])
    # for rel_id in rels_id_test:
    #     triples_used = triples_used | triples_2_str_set(rel2triples_id[rel_id])  

    # print('task used triples', len(triples_used))

    # triples_id_all = read_triples_id(file_id)
    # print('all triples', len(triples_id_all))
    # triples_background_id = []
    # triples_background_str = []
    # for index, triple_id in enumerate(triples_id_all):
    #     (h, r, t, s) = triple_id
    #     tmp_str = h + ',' + r + ',' + t
    #     if tmp_str not in triples_used:
    #         triples_background_id.append((h, r, t, s))
    #         triples_background_str.append((id2ent[h], id2rel[r], id2ent[t], s))
    
    # print('background triples', len(triples_background_id))

    # write_triples(out_path + 'path_graph', triples_background_str)




if __name__ == "__main__":
    in_path = './NL27K_with_neg_res/'
    out_path = './NL27K-tasks/NL27K-N0/'

    ent2id, id2ent = read_symbol2id(in_path + 'entity2id.tsv')
    rel2id, id2rel = read_symbol2id(in_path + 'relation2id.tsv')

    pos_triples_id = read_triples_id(in_path + 'n0.tsv')
    rel2candidates = candidate_triples(pos_triples_id) # 构造type constraint

    #---------------- 划分数据集 ---------------- 

    train_tasks = list(json.load(open('./NL27K-N3/train_tasks.json')))
    dev_tasks = list(json.load(open('./NL27K-N3/dev_tasks.json')))
    test_tasks = list(json.load(open('./NL27K-N3/test_tasks.json')))

    rels_id_train = [rel2id[rel_str] for rel_str in train_tasks]
    rels_id_val = [rel2id[rel_str] for rel_str in dev_tasks]
    rels_id_test = [rel2id[rel_str] for rel_str in test_tasks]


    out_path = './NL27K-tasks/NL27K-N0/'
    create_tasks(in_path + 'n0.tsv', out_path)

    out_path = './NL27K-tasks/NL27K-N1/'
    create_tasks(in_path + 'n1.tsv', out_path)

    out_path = './NL27K-tasks/NL27K-N2/'
    create_tasks(in_path + 'n2.tsv', out_path)

    out_path = './NL27K-tasks/NL27K-N3/'
    create_tasks(in_path + 'n3.tsv', out_path)



