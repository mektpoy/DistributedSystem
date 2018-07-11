# lab2
计55 许翰翔 2015011370

## 实验流程

先使用GraphBuilder.java将网页以邻接表的格式处理出来。使用正则表达式匹配xml代码：

```java
    private static final String TitleRegex = "<title>.*</title>";
    private static final String LinkRegex = "\\[\\[[^\\[\\]]*\\]\\]";
```

其中对标题做一遍hash。

```java
    public String hash(String str) {
            char s[] = str.toCharArray();
            int len = str.length();
            long ret = 0;
            for (int i = 0; i < len; i ++)
                ret = ret * 31 + s[i];
            return String.valueOf(ret);
        }
```

而对于网页中出现的\[\[abcd\|efgh\]\]形式，分析后发现\|的左边才是有用的，特殊处理。
这一步的结果保存在hdfs用户空间下的~/pagerank1中

数据的大致格式如下

```
    hash_title1 \t hash_titleA,hash_titleB,hash_titleC
    hash_title2 \t hash_titleD,hash_titleE
    hash_title3 \t
```

之后使用PageRanker.java，每次运行时做一次迭代，map过程将邻接表原封不动传给reducer，对于每个出度均匀分配当前节点的PR权值；在reducer阶段计算获得的PR权值之和，作为下一轮开始的PR值。

其中参数alpha取0.15：

```java
    public static class PageReducer extends Reducer <Text, Text, Text, Text> {
    private Text outputVal = new Text();
    private static final double alpha = 0.15;

    @Override
    public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException {
            boolean flag = false;
            String str = "";
            String LinkStr = "";
            double total = 0;

            for (Text value : values) {
                str = value.toString();
                if (str.startsWith("|")) {
                    flag = true;
                    LinkStr = str.substring(1);
                    continue;
                }
                double GetRank = Double.valueOf(str);
                total += GetRank;
            }

            if (!flag) {
                return;
            }

            double Rank = (1.0 - alpha) * total + alpha;
            outputVal.set(String.valueOf(Rank) + ":" + LinkStr);
            context.write(key, outputVal);
        }
    }
```



## Write-up
- How long do you think this project would take you to finish?

20h

- How much time did you actually spend on this project?

3days++++++

- Acknowledge any assistance you received from anyone except assigned course readings and the course staff.

全部独立完成。

- Explain why it's ok to use a combiner class with the PageRank algorithm.

因为pagerank的操作实际上是在收集算术和，这显然是满足结合律的，所以可以使用combiner。

- What are the ten pages with highest rank in the provided Wikipedia corpus?

因为我之前处理的时候做了一步hash，要解码成原标题才能做这题，发现的时候已经太迟了来不及了，所以这个需要过几个小时才能知道…（检查的时候应该已经知道了）

- Describe any extensions you've implemented.
扩展3：Link Hack Wikipedia. Use your data to subtly edit important Wikipedia pages to increase the pagerank of a pet cause. Tell us what you did and why it should work.


原本的PageRank算法，对于一个具有k个出度的节点，将自身权值均分成k份，传至下一轮的值，于是这里只需要做一个加权的传递即可，具体的说，将带有pet关键词的节点权值看作2，其余节点看作1，计算k个节点的总权值d，则每个出节点获得的新值为1 or 2 / d * PR[x]

对PageRank的map函数大致做如下修改即可：

```java
    String[] LinkList = LinkStr.split(",");
    int tot = 0;
    for (String Link : LinkList) {
        if (Link is about pet) {
            tot += 2;
        } else {
            tot += 1;
        }
    }
    double val = Double.valueOf(Rank) / tot;
    for (String Link : LinkList) {
        outputKey.set(Link);
        if (Link is about pet) {
            outputVal.set(String.valueOf(2.0 * val));
        } else {
            outputVal.set(String.valueOf(val));
        }
        context.write(outputKey, outputVal);
    }
```

扩展4：Implement variably weighted links. Explain your heuristic and convince us that its working.

这个跟扩展3处理的方式类似，实质上还是k个出节点怎么分唯一的一块蛋糕的问题，比如说我比较关注sports的链接，那我就将sports划分的权值多一些，比如是普通的3倍之类的。PageRank算法每次迭代，所有节点的PR权值和是不会变的，这样会导致我们关注的节点获得相对更多的权值。