
#include<iostream>
#include<cstring>
#include<cstdio>
#include<map>
#include<vector>
#include<string>
#include<ctime>
#include<cmath>
#include<cstdlib>
#include<sstream>
using namespace std;


#define pi 3.1415926535897932384626433832795

bool L1_flag=1;

int nepoch = 100;
string transE_version = "bern";
double conf_step = 0.0001;		//rate_confidence

double lamda0 = 1.5;		//rate conf
double lamda1 = 0.1;		//hard conf
double lamda2 = 0.4;		//soft conf

//normal distribution
double rand(double min, double max)
{
    return min+(max-min)*rand()/(RAND_MAX+1.0);
}
double normal(double x, double miu,double sigma)
{
    return 1.0/sqrt(2*pi)/sigma*exp(-1*(x-miu)*(x-miu)/(2*sigma*sigma));
}
double randn(double miu,double sigma, double min ,double max)
{
    double x,y,dScope;
    do{
        x=rand(min,max);
        y=normal(x,miu,sigma);
        dScope=rand(0.0,normal(miu,miu,sigma));
    }while(dScope>y);
    return x;
}

double sqr(double x)
{
    return x*x;
}

double vec_len(vector<double> &a)
{
	double res=0;
    for (int i=0; i<a.size(); i++)
		res+=a[i]*a[i];
	res = sqrt(res);
	return res;
}

string version;
char buf[100000],buf1[100000];
int relation_num,entity_num;
map<string,int> relation2id,entity2id;		//<relation,ID>
map<int,string> id2entity,id2relation;		//<ID,relation>

//confidence
vector<double> rate_confidence;		// local triple confidence
vector<double> hard_confidence;		// prior path confidence
vector<double> soft_confidence;		// adaptive path confidence

map<pair<string,int>,double>  path_confidence; // 看完代码，这个东西实际没有被使用，用途不明
map<vector<int>,string> path2s;


map<int,map<int,int> > left_entity,right_entity;		//<relationID, <entityID, num>>
map<int,double> left_num,right_num;

class Train{

public:
    vector<int> fb_h,fb_l,fb_r;		//headID��tailID��relationID��training set
	map<pair<int,int>, map<int,int> > ok;
	//training set
    void add(int x,int y,int z, vector<pair<vector<int>,double> > path_list)
    {
        fb_h.push_back(x);
        fb_r.push_back(z);
        fb_l.push_back(y);
		fb_path.push_back(path_list);
        ok[make_pair(x,z)][y]=1;
    }
	//train
    void run(int n_in,double rate_in,double margin_in,int method_in)
    {
        n = n_in;
        rate = rate_in;
        margin = margin_in;
        method = method_in;
        relation_vec.resize(relation_num);		//relation_vec
		for (int i=0; i<relation_vec.size(); i++)
			relation_vec[i].resize(n);
        entity_vec.resize(entity_num);		//entity_vec
		for (int i=0; i<entity_vec.size(); i++)
			entity_vec[i].resize(n);
        relation_tmp.resize(relation_num);		//relation_tmp
		for (int i=0; i<relation_tmp.size(); i++)
			relation_tmp[i].resize(n);
        entity_tmp.resize(entity_num);		//entity_tmp
		for (int i=0; i<entity_tmp.size(); i++)
			entity_tmp[i].resize(n);
		//initialization, pre-trained TransE
		/*
		FILE* f1 = fopen(("../transE_res/entity2vec."+transE_version).c_str(),"r");
		for (int i=0; i<entity_num; i++)
		{
			for (int ii=0; ii<n; ii++)
				fscanf(f1,"%lf",&entity_vec[i][ii]);
			norm(entity_vec[i]);
		}
		fclose(f1);
		FILE* f2 = fopen(("../transE_res/relation2vec."+transE_version).c_str(),"r");
		for (int i=0; i<relation_num; i++)
		{
			for (int ii=0; ii<n; ii++)
			{
				fscanf(f2,"%lf",&relation_vec[i][ii]);
				//relation_vec[i+1345][ii] = -relation_vec[i][ii];
			}
		}
		fclose(f2);
		*/
		// initialization random
		// cout << "_relation_num" << relation_num << endl;
		// cout << "_entity_num" << entity_num << endl;
        for (int i=0; i<relation_num; i++)
        {
            for (int ii=0; ii<n; ii++)
                relation_vec[i][ii] = randn(0,1.0/n,-6/sqrt(n),6/sqrt(n));
        }
        for (int i=0; i<entity_num; i++)
        {
            for (int ii=0; ii<n; ii++)
                entity_vec[i][ii] = randn(0,1.0/n,-6/sqrt(n),6/sqrt(n));
            norm(entity_vec[i]);
        }

		rate_confidence.resize(fb_h.size());		//initialization
		soft_confidence.resize(fb_h.size());		//initialization
		for(int i=0; i<fb_h.size(); i++)
		{
			rate_confidence[i] = 1;
			soft_confidence[i] = 0;
		}
		cout << "[INFO]" << " model init finished" << endl;
        sgd();
    }

private:
    int n,method;		//n:embedding
    double res;		//loss function value
    double count,count1;
    double rate,margin;	
    double belta;

	vector<vector<pair<vector<int>,double> > > fb_path;
    vector<vector<int> > feature;
    vector<vector<double> > relation_vec,entity_vec;		//relation/entity embedding
    vector<vector<double> > relation_tmp,entity_tmp;
    double norm(vector<double> &a)
    {
        double x = vec_len(a);
        if (x>1)
        for (int ii=0; ii<a.size(); ii++)
                a[ii]/=x;
        return 0;
    }
    int rand_max(int x)
    {
        int res = (rand()*rand())%x;
        while (res<0)
            res+=x;
        return res;
    }
	double calc_path(int r1,vector<int> rel_path)		//calc ||p-r||
    {
        double sum=0;
        for (int ii=0; ii<n; ii++)
		{
			double tmp = relation_vec[r1][ii];
			for (int j=0; j<rel_path.size(); j++)
			{
				if(rel_path[j] < 417)
					tmp-=relation_vec[rel_path[j]][ii];
				else
					tmp+=relation_vec[rel_path[j]-417][ii];
			}
	        if (L1_flag)
				sum+=fabs(tmp);
			else
				sum+=sqr(tmp);
		}
        return sum;
    }
    void sgd()		//train
    {
		// cout << "fb_h size = " << fb_h.size() << endl;
        res=0;
        int nbatches=1000;
        int batchsize = fb_h.size() / nbatches;		//batch size
		for (int epoch=0; epoch<nepoch; epoch++)
		{
			res=0;
			// cout << "epoch start" << endl;
			for (int batch = 0; batch<nbatches; batch++)
			{
				relation_tmp=relation_vec;
				entity_tmp = entity_vec;
				for (int k = 0; k<batchsize; k++)
				{
					int i=rand_max(fb_h.size());
					int j=rand_max(entity_num);
					// cout << "flag1" << endl;
					double pr = 1000*right_num[fb_r[i]]/(right_num[fb_r[i]]+left_num[fb_r[i]]);
					if (method ==0)
						pr = 500;
					if (rand()%1000<pr)
					{
						while (ok[make_pair(fb_h[i],fb_r[i])].count(j)>0)
							j=rand_max(entity_num);
						train_kb(fb_h[i],fb_l[i],fb_r[i],fb_h[i],j,fb_r[i],i);
					}
					else
					{
						while (ok[make_pair(j,fb_r[i])].count(fb_l[i])>0)
							j=rand_max(entity_num);
						train_kb(fb_h[i],fb_l[i],fb_r[i],j,fb_l[i],fb_r[i],i);
					}
					// cout << "flag2" << endl;
					int rel_neg = rand_max(relation_num);
					while (ok[make_pair(fb_h[i],rel_neg)].count(fb_l[i])>0)
						rel_neg = rand_max(relation_num);
					train_kb(fb_h[i],fb_l[i],fb_r[i],fb_h[i],fb_l[i],rel_neg,i);
					// cout << "flag2.5" << endl;
					double all_path_conf = 0;
					if (fb_path[i].size()>0)
					{
						for (int path_id = 0; path_id<fb_path[i].size(); path_id++)		//multiple entity path
						{
							vector<int> rel_path = fb_path[i][path_id].first;
							double pr = fb_path[i][path_id].second;		//entity pair->path
							double sum = calc_path(fb_r[i], rel_path);
							all_path_conf += pr / sum;		//[0, 1]
							// cout << "flag count" << endl;
						}
						soft_confidence[i] = 1.0 / (1 + exp(-all_path_conf));		//sigmoid
					}
					// cout << "flag3" << endl;
					norm(relation_tmp[fb_r[i]]);
					norm(relation_tmp[rel_neg]);
					norm(entity_tmp[fb_h[i]]);
					norm(entity_tmp[fb_l[i]]);
					norm(entity_tmp[j]);
					// cout << "flag4" << endl;
				}
				relation_vec = relation_tmp;
				entity_vec = entity_tmp;
			}
			cout<<"epoch:"<<epoch<<' '<<res<<endl;
			FILE* f4 = fopen("./NL27K_with_neg_res/train_LT_conf.txt","w");
			FILE* f5 = fopen("./NL27K_with_neg_res/train_AP_conf.txt","w");
			FILE* f6 = fopen("./NL27K_with_neg_res/train_PP_conf.txt","w");
			FILE* f7 = fopen("./NL27K_with_neg_res/train_id.tsv","w");
			float tmp_confidence;
			for (int i=0; i<fb_h.size(); i++)		//output rate_confidence
			{
				fprintf(f4,"%.6lf\n",rate_confidence[i]);
			}
			for (int i=0; i<fb_h.size(); i++)		//output soft_confidence
			{
				fprintf(f5,"%.6lf\n",soft_confidence[i]);
			}
			for (int i=0; i<fb_h.size(); i++)		//output hard_confidence
			{
				fprintf(f6,"%.6lf\n",hard_confidence[i]);
			}
			for (int i=0; i<fb_h.size(); i++)		//output train_triples_id
			{
				tmp_confidence = (lamda0*rate_confidence[i] + lamda1*hard_confidence[i] + lamda2*soft_confidence[i]) / 2.0;
				fprintf(f7,"%d\t%d\t%d\t%.6lf\n", fb_h[i], fb_r[i], fb_l[i], tmp_confidence);
			}
			// fclose(f2);
			// fclose(f3);
			fclose(f4);
			fclose(f5);
			fclose(f6);
			fclose(f7);
		}
    }
    double calc_sum(int e1,int e2,int rel)
    {
        double sum=0;
        if (L1_flag)
        	for (int ii=0; ii<n; ii++)
            	sum+=fabs(entity_vec[e2][ii]-entity_vec[e1][ii]-relation_vec[rel][ii]);		//h+r=t
        else
        	for (int ii=0; ii<n; ii++)
            	sum+=sqr(entity_vec[e2][ii]-entity_vec[e1][ii]-relation_vec[rel][ii]);		//h+r=t
        return sum;
    }
    void gradient(int e1_a,int e2_a,int rel_a,int e1_b,int e2_b,int rel_b,double tri_conf)		//update
    {
        for (int ii=0; ii<n; ii++)
        {

            double x = 2*(entity_vec[e2_a][ii]-entity_vec[e1_a][ii]-relation_vec[rel_a][ii]);
            if (L1_flag)
            	if (x>0)
            		x=1;
            	else
            		x=-1;
			x = rate*x*tri_conf;
            relation_tmp[rel_a][ii]+=x;
            entity_tmp[e1_a][ii]+=x;
            entity_tmp[e2_a][ii]-=x;
            x = 2*(entity_vec[e2_b][ii]-entity_vec[e1_b][ii]-relation_vec[rel_b][ii]);
            if (L1_flag)
            	if (x>0)
            		x=1;
            	else
            		x=-1;
			x = rate*x*tri_conf;
            relation_tmp[rel_b][ii]-=x;
            entity_tmp[e1_b][ii]-=x;
            entity_tmp[e2_b][ii]+=x;
        }
    }

    void train_kb(int e1_a,int e2_a,int rel_a,int e1_b,int e2_b,int rel_b,int tri_num)
    {
		// cout << "in train_kb" << endl;
        double sum1 = calc_sum(e1_a,e2_a,rel_a);
        double sum2 = calc_sum(e1_b,e2_b,rel_b);
		// cout << "train_kb 1.2" << endl;
        if(sum1+margin>sum2)
        {
        	res+=margin+sum1-sum2;
			double temp_conf = lamda0*rate_confidence[tri_num] + lamda1*hard_confidence[tri_num] + lamda2*soft_confidence[tri_num];
        	gradient( e1_a, e2_a, rel_a, e1_b, e2_b, rel_b, temp_conf);
        }
		// cout << "train_kb 1.5" << endl;
		//update rate_confidence
		if(sum1+margin>sum2)
		{
			rate_confidence[tri_num] *= 0.9;
			if(rate_confidence[tri_num] < 0.0)
				rate_confidence[tri_num] = 0.0;
		}
		else if(sum1+margin<=sum2)
		{
			rate_confidence[tri_num] += conf_step;
			if(rate_confidence[tri_num] > 1.0)
				rate_confidence[tri_num] = 1.0;
		}
		// cout << "out train_kb" << endl;
    }
};

Train train;
void prepare()
{
    FILE* f1 = fopen("./NL27K_with_neg/entity2id.tsv","r");
	FILE* f2 = fopen("./NL27K_with_neg/relation2id.tsv","r");
	int x;
	//build entity2ID and ID2entity map
	while (fscanf(f1,"%d%s",&x,buf)==2)
	{
		string st=buf;
		entity2id[st]=x;		//<entity,ID>
		id2entity[x]=st;		//<ID,entity>
		entity_num++;
	}
	//build relation2ID andID2relation map
	while (fscanf(f2,"%d%s",&x,buf)==2)
	{
		string st=buf;
		relation2id[st]=x;
		id2relation[x]=st;
		relation_num++;
	}
	// 读取训练集及其路径信息
	FILE* f_kb = fopen("./NL27K_with_neg/pos_with_neg_pra.txt","r");
	bool neg_triple = false;
	// cout << "fb_h size (-1) = " << train.fb_h.size() << endl;
	// cout << "pra size(-1) = " << train.ok.size() << endl;
	int cnt = 0;
	while (fscanf(f_kb,"%s",buf)==1)
    {
		cnt += 1;
        string s1=buf;
        fscanf(f_kb,"%s",buf);
        string s2=buf;
        if (entity2id.count(s1)==0)
        {
            cout<<"miss entity:"<<s1<<endl;
        }
        if (entity2id.count(s2)==0)
        {
            cout<<"miss entity:"<<s2<<endl;
        }
        int e1 = entity2id[s1];
        int e2 = entity2id[s2];
        int rel;
		fscanf(f_kb,"%d",&rel);
		if(rel >= 417)		//reverse triple
			neg_triple = true;
		else
			neg_triple = false;
		fscanf(f_kb,"%d",&x);
		vector<pair<vector<int>,double> > b;		//path vector
		b.clear();
		for (int i = 0; i < x; i++)
		{
			int y,z;
			vector<int> rel_path;
			rel_path.clear();
			fscanf(f_kb,"%d",&y);
			for (int j=0; j<y; j++)
			{
				fscanf(f_kb,"%d",&z);
				rel_path.push_back(z);
			}
			double pr;
			fscanf(f_kb,"%lf",&pr);
			if(rel_path.size() == 1 && rel_path[0] == rel)		//remove current relation
				continue;
			b.push_back(make_pair(rel_path,pr));		//<relation_path, score>
		}
		// cout<<s1<<' '<<s2<<' '<<rel<<' '<<b.size()<<endl;
		if(neg_triple)		//remove triple
			continue;
        train.add(e1,e2,rel,b);
		left_entity[rel][e1]++;		//build <relationID, <entityID, num>>
		right_entity[rel][e2]++;		//build <relationID, <entityID, num>>
    }
	cout << "pra size = " << train.ok.size() << endl;
	cout << "fb_h size = " << train.fb_h.size() << endl;
	// cout << cnt << endl;
	//read prior path confidence
	double y;
	f_kb = fopen("./NL27K_with_neg/pos_with_neg_PP_conf.txt","r");
	while (fscanf(f_kb,"%lf",&y)==1)
	{
		hard_confidence.push_back(y);
	}
	cout << "hard triple confidence = " << hard_confidence.size() << endl;

    for (int i=0; i<relation_num; i++)
    {
    	double sum1=0,sum2=0;
    	for (map<int,int>::iterator it = left_entity[i].begin(); it!=left_entity[i].end(); it++)
    	{
    		sum1++;
    		sum2+=it->second;
    	}
    	left_num[i]=sum2/sum1;
    }
    for (int i=0; i<relation_num; i++)
    {
    	double sum1=0,sum2=0;
    	for (map<int,int>::iterator it = right_entity[i].begin(); it!=right_entity[i].end(); it++)
    	{
    		sum1++;
    		sum2+=it->second;
    	}
    	right_num[i]=sum2/sum1;
    }
    cout<<"relation_num = "<<relation_num<<endl;
    cout<<"entity_num = "<<entity_num<<endl;
	fclose(f_kb);
}

int ArgPos(char *str, int argc, char **argv) {
  int a;
  for (a = 1; a < argc; a++) if (!strcmp(str, argv[a])) {
    if (a == argc - 1) {
      printf("Argument missing for %s\n", str);
      exit(1);
    }
    return a;
  }
  return -1;
}

int main(int argc,char**argv)
{
    srand((unsigned) time(NULL));
    int method = 1;
    int n = 50;		//dimention
    double rate = 0.001;		//learning rate
    double margin = 1;		//loss margin
	if (method)
        version = "bern";
    else
        version = "unif";
    cout<<"dim size = "<<n<<endl;
    cout<<"learing rate = "<<rate<<endl;
    cout<<"margin = "<<margin<<endl;
    cout <<"method = "<<version<<endl;
    prepare();
	cout << "[INFO] data load finished" << endl;
    train.run(n,rate,margin,method);
}


