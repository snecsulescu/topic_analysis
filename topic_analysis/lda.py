import optparse
import sys
from collections import defaultdict

from pyspark.context import SparkContext
from pyspark.mllib.clustering import LDA
from pyspark.mllib.linalg import Vectors

import os
import re
from topic_analysis.utils import eliminated_repeated_chars
from gensim.parsing import STOPWORDS

sc = SparkContext()


def document_vector(doc, vocab):
    id = doc[1]
    counts = defaultdict()
    for token in doc[0]:
        if token in vocab:
            token_id = vocab[token]
            if token_id in counts:
                counts[token_id] += 1
            else:
                counts[token_id] = 1

    counts = sorted(counts.items())
    keys = [x[0] for x in counts]
    values = [x[1] for x in counts]
    return (id, Vectors.sparse(len(vocab), keys, values))


def get_lemma(word_lemma):
    print word_lemma
    lemma = word_lemma.rsplit("/", 1)[0]
    print(lemma)
    return lemma


if __name__ == "__main__":
    parser = optparse.OptionParser()
    parser.add_option('-d', '--dir',
                      dest="dir",
                      type="string",
                      help="The data directory."
                      )
    parser.add_option('-o', '--dirout',
                      dest="dirout",
                      type="string",
                      help="The output directory."
                      )
    options, remainder = parser.parse_args()

    if not hasattr(options, "dir") or options.dir is None:
        sys.exit("You must enter the input directory path.")

    if not hasattr(options, "dirout") or options.dirout is None:
        sys.exit("You must enter the output directory path.")

    if not options.dirout.endswith("/"):
        options.dirout += "/"

    if not os.path.exists(options.dirout):
        os.makedirs(options.dirout)

    # Load and parse the data
    data = sc.wholeTextFiles(options.dir + "/*.txt")
    docs = data.map(lambda doc: doc[1].strip().lower()) \
        .map(lambda doc: re.split("[\s;,#]", eliminated_repeated_chars(doc))) \
        .map(lambda doc: [word for word in doc if re.match("^[a-zA-Z][\w&\-]*[a-zA-Z]$", word)]) \
        .map(lambda doc: [word for word in doc if not (word in STOPWORDS)]) \
        .map(lambda doc: [word for word in doc if len(word) > 2])

    termCounts = docs.flatMap(lambda words: words) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda x, y: x + y) \

    termCounts = termCounts.filter(lambda tuple: tuple[1] > 1) \
        .map(lambda tuple: (tuple[1], tuple[0])) \
        .sortByKey(False)

    vocab = termCounts.map(lambda tuple: tuple[1]) \
        .zipWithIndex() \
        .collectAsMap()

    documents = docs.zipWithIndex().map(lambda doc: document_vector(doc, vocab)).map(list)

    inv_voc = {value: key for (key, value) in vocab.items()}

    with open("lda_output.txt", 'w') as f:
        lda_model = LDA.train(documents, k=13, maxIterations=100)

        topic_indices = lda_model.describeTopics(maxTermsPerTopic=100)
        print topic_indices
        for i in range(len(topic_indices)):
            f.write("Topic #{0}\n".format(i + 1))
            for j in range(len(topic_indices[i][0])):
                print("{0}\t{1}\n".format(inv_voc[topic_indices[i][0][j]].encode('utf-8'), topic_indices[i][1][j]))
                f.write("{0}\t{1}\n".format(inv_voc[topic_indices[i][0][j]].encode('utf-8'), topic_indices[i][1][j]))
        print(
            "{0} topics distributed over {1} documents and {2} unique words\n".format(5, documents.count(), len(vocab)))
        f.write(
            "{0} topics distributed over {1} documents and {2} unique words\n".format(5, documents.count(), len(vocab)))
