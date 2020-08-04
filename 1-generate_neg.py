'''
利用正例生成负例
'''
import random
from collections import defaultdict

in_path = './1-NL27K_base/'
out_path = './2-NL27K_with_neg/'

low_freq = 50
up_freq = 500

ent2id = None
id2ent = None
rel2id = None
id2rel = None
postive_set = None
ent2cnt = None
rel2cnt = None
# pos_triples = None # 原本数据集(正例)

def read_triples_id(file):
    '''
    read triple file (h, r, t)
    '''
    triples = []
    with open(file) as f:
        lines = f.readlines()
        for line in lines:
            (h, r, t) = line.replace('\n', '').split('\t')
            triples.append((h, r, t)) # (h, r, t)
    f.close()   
    return triples

def write_triples(file, triples):
    '''
    write triples file
    '''
    with open(file, 'w') as f:
        for triple in triples:
            (h, r, t) = triple
            line = id2ent[h] + '\t' + id2rel[r] + '\t' + id2ent[t] + '\n'
            f.write(line)
    f.close()

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
            tmp = line.replace('\n', '').split(',')
            symbol_id, symbol_str = tmp[0], tmp[1]
            symbol2id[symbol_str] = symbol_id
            id2symbol[symbol_id] = symbol_str

    return symbol2id, id2symbol

def count_freq(triples):
    '''
    统计每个关系在triples中出现的频率
    '''
    ent2cnt = {}
    rel2cnt = {}

    for triple in triples:
        (h, r, t) = triple
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

def triples_2_str_set(triples):
    '''
    将一个triple_list中的triple都搞成一个string并且丢到一个set中
    '''
    triple_set = set()
    for triple in triples:
        (h, r, t) = triple
        tmp_str = h + ',' + r + ',' + t
        triple_set.add(tmp_str)
    return triple_set


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

def candidate_triples(pos_triples):
    '''
    生成每个relation的candidate set
    '''
    all_entities = ent2id.keys()

    type2ents = defaultdict(set)
    for ent in all_entities:
        type_ = ent.split(':')[1]
        type2ents[type_].add(ent)


    pos_triples_str = [(id2ent[h], id2rel[r], id2ent[t]) for (h, r, t) in pos_triples]
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

def filter_relation_freq(triples):
    '''
    统计triples中relation的频率，通保留在目标频率区间中的relation
    ps:50-500范围内有134个关系
    '''
    tmp_list = sorted(rel2cnt.items(), key = lambda x:x[1], reverse = False)
    rel_list = []
    for (rel_id, freq) in tmp_list:
        if freq >= low_freq and freq <= up_freq:
            rel_list.append(rel_id)
    return rel_list

def generate_neg_examples(pos_triples, neg_num):
    '''
    利用正例生成负例，修改方案，只需要对于不同的r来定制不同的candidate set就ok
    '''
    neg_triples = []
    neg_set =  set()
    # 获取一些重要资源
    rel2candidates = candidate_triples(pos_triples) # 构造type constraint
    few_rel = filter_relation_freq(pos_triples)
    print('[INFO] Few relation num', len(few_rel))

    pos_indexs = random.sample(range(len(pos_triples)),neg_num) # 随机采样neg_num个正例，后面需要对这些正例打乱
    # print('随机数产生完成')
    for i, pos_index in enumerate(pos_indexs):
        (h, r, t) = pos_triples[pos_index]
        while True:
            candidates = rel2candidates[id2rel[r]]
            # print(id2rel[r], len(candidates))
            noise = random.choice(candidates)
            noise = ent2id[noise]
            key_str = h + ',' + r + ',' + noise
            if key_str not in postive_set and key_str not in neg_set and noise != t:
                neg_triples.append((h, r, noise))
                neg_set.add(key_str)
                break

    print('[INFO] 生成负例数量', len(neg_triples))
    return neg_triples

if __name__ == "__main__":
    pos_triples = read_triples_id(in_path + 'data_id.tsv')
    print('[INFO] 载入正例数量:', len(pos_triples))
    postive_set = triples_2_str_set(pos_triples)
    ent2id, id2ent = read_symbol2id(in_path + 'entity_id.csv')
    rel2id, id2rel = read_symbol2id(in_path + 'relation_id.csv')
    ent2cnt, rel2cnt = count_freq(pos_triples)
    # 构建负例数量
    n1_neg_num = int(len(pos_triples) * 0.4) # 40% 比例的负例
    # 构造负例
    n1_neg_triples = generate_neg_examples(pos_triples, n1_neg_num)
    write_triples(out_path + 'n1_neg.tsv', n1_neg_triples) # 负例文件
    write_triples(out_path + 'pos_with_neg.tsv', pos_triples + n1_neg_triples) # 正例和负例




