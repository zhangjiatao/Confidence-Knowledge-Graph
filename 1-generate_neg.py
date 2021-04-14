'''
Step 1: Generate nagative examples
'''
import random
from collections import defaultdict
from shutil import copyfile

in_path = './1-data_base/'
out_path = './2-data_with_neg/'

ent2id = None
id2ent = None
rel2id = None
id2rel = None
postive_set = None # 用于去除构造的负例中的正例三元组


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
            tmp = line.replace('\n', '').split('\t')
            symbol_id, symbol_str = tmp[0], tmp[1]
            symbol2id[symbol_str] = symbol_id
            id2symbol[symbol_id] = symbol_str

    return symbol2id, id2symbol


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


def generate_neg_examples(pos_triples, neg_num):
    '''
    利用正例生成负例，修改方案，只需要对于不同的r来定制不同的candidate set就ok
    '''
    neg_triples = []
    neg_set =  set()
    # 获取一些重要资源
    rel2candidates = candidate_triples(pos_triples) # 构造type constraint

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
    # read data files
    pos_triples = read_triples_id(in_path + 'data_id.tsv')
    ent2id, id2ent = read_symbol2id(in_path + 'entity_id.tsv')
    rel2id, id2rel = read_symbol2id(in_path + 'relation_id.tsv')

    print('[INFO] 载入正例数量:', len(pos_triples))
    postive_set = triples_2_str_set(pos_triples)
    # 构建负例数量
    neg_num = int(len(pos_triples) * 0.4) # 40% 比例的负例
    # 构造负例
    neg_triples = generate_neg_examples(pos_triples, neg_num)
    write_triples(out_path + 'neg_data.tsv', neg_triples) # 负例文件
    write_triples(out_path + 'data_with_neg.tsv', pos_triples + neg_triples) # 正例和负例
    # copy file
    copyfile(in_path + 'entity_id.tsv', out_path + 'entity_id.tsv')
    copyfile(in_path + 'relation_id.tsv', out_path + 'relation_id.tsv')



