'''
score统计和均衡均衡化
'''
import numpy as np
import torch
import math

in_path = './NL27K_with_neg_res/'
out_path = './NL27K_with_neg_res/'


def recorrect_score(x):
    x = 6 * x
    x = 1 - (2 / (1 + math.exp(2 * x)))
    return x

def read_triples_id(file):
    '''
    read triple file
    '''
    triples = []
    with open(file) as f:
        lines = f.readlines()
        for line in lines:
            tmp = line.replace('\n', '').split('\t')
            triples.append((tmp[0], tmp[1], tmp[2], float(tmp[3]))) # (h, r, t, s),
    f.close()   
    return triples

def write_triples(file, triples):
    '''
    write triples file
    '''
    with open(file, 'w') as f:
        for triple in triples:
            (h, r, t, s) = triple
            s = str(recorrect_score(float(s)))
            line = h + '\t' + r + '\t' + t + '\t' + s + '\n'
            f.write(line)
    f.close()


def sigma(x):
    '''
    对应原论文的sigma函数（UOKGE/doc/Report.pdf中的公式(1)）
    '''
    return 1 - (2 / (1 + np.exp(2 * x)))


def mean_and_std_dataset(file):
    '''
    统计dataset的socre的均值和标准差
    并进行数据修正
    '''
    triples = read_triples_id(file)
    scores = np.array(triples, dtype = float)[:,3]
    
    # # 进行数据修正
    # scores = scores * 6
    # scores = sigma(scores)

    pos_scores = scores[:175412]
    neg_scores = scores[175412:]
    mean_score_all = np.mean(scores)
    std_score_all = np.std(scores)
    mean_score_pos = np.mean(pos_scores)
    std_score_pos = np.std(pos_scores)
    mean_score_neg = np.mean(neg_scores)
    std_score_neg = np.std(neg_scores)
    print('------- after recorrect:%s ---------' % file)
    print('data size:', scores.shape[0])
    print('Max value:', scores.max())
    print('Min value:', scores.min())
    print('Over All | Mean: %.4lf | Std: %.4lf' % (mean_score_all, std_score_all))
    print('Positive | Mean: %.4lf | Std: %.4lf' % (mean_score_pos, std_score_pos))
    print('Negative | Mean: %.4lf | Std: %.4lf' % (mean_score_neg, std_score_neg))

    return scores

def divide_dataset(file):
    '''
    按照不同比例划分n1(10%负例),n2(20%),n3(40%)
    '''
    triples = read_triples_id(file)
    pos_num = 175412
    n1_num = int(pos_num + pos_num * 0.1)
    n2_num = int(pos_num + pos_num * 0.2)
    n3_num = int(pos_num + pos_num * 0.4)
    write_triples(out_path + 'n0.tsv', triples[:175412])
    write_triples(out_path + 'n1.tsv', triples[:n1_num])
    write_triples(out_path + 'n2.tsv', triples[:n2_num])
    write_triples(out_path + 'n3.tsv', triples[:n3_num])
    
if __name__ == "__main__":
    # mean_and_std_dataset(in_path + 'data_n3.tsv')
    divide_dataset(in_path + 'data_n3.tsv')
    mean_and_std_dataset(in_path + 'n0.tsv')
    mean_and_std_dataset(in_path + 'n1.tsv')
    mean_and_std_dataset(in_path + 'n2.tsv')
    mean_and_std_dataset(in_path + 'n3.tsv')