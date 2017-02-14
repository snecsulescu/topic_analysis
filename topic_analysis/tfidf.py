import cPickle
import optparse
import sys

from pyspark.context import SparkContext
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import IDF
from pyspark.sql import SQLContext

import os
import re
from topic_analysis import utils
from topic_analysis.utils import eliminated_repeated_chars
from gensim.parsing import STOPWORDS

sc = SparkContext()
sqlContext = SQLContext(sc)


def analyse(doc, vocab, ont):
    """ Analyze the given report and returns the total amount of tfidf scores, the total amount of TfIdf score
    per ontology topic and the words that belong to each topic
    :param doc: the report to be analized
    :param vocab: the vocabulary for which the TfIdf score is calculated
    :param ont: the given ontology
    :return:
    """
    print(doc[0])
    sum = 0.0
    sum_ont = {}
    terms_ont = {}
    for i, token in enumerate(vocab):
        if doc[1][i] > 0.0:
            for topic in ont:
                if token in ont[topic]:
                    if not (topic in sum_ont):
                        sum_ont[topic] = 0.0
                        terms_ont[topic] = []
                    sum_ont[topic] += doc[1][i]
                    terms_ont[topic].append((token, doc[1][i]))
            sum += doc[1][i]
    return doc[0], sum, sum_ont, terms_ont


if __name__ == "__main__":
    parser = optparse.OptionParser()
    parser.add_option('-d', '--dir',
                      dest="dir",
                      type="string",
                      help="The data directory."
                      )
    parser.add_option('-t', '--ont',
                      dest="ontology",
                      type="string",
                      help="The ontology file."
                      )

    parser.add_option('-o', '--dirout',
                      dest="dirout",
                      type="string",
                      help="The output directory."
                      )
    options, remainder = parser.parse_args()

    if not hasattr(options, "dir") or options.dir is None:
        sys.exit("You must enter the main directory path where the reports can be found.")

    if not hasattr(options, "dirout") or options.dirout is None:
        sys.exit("You must enter the output directory path.")

    if not hasattr(options, "ontology") or options.ontology is None:
        sys.exit("You must enter the ontology file.")

    if not options.dir.endswith("/"):
        options.dir += "/"

    if not options.dirout.endswith("/"):
        options.dirout += "/"

    if not os.path.exists(options.dirout):
        os.makedirs(options.dirout)

    # Load and parse the data
    data = sc.wholeTextFiles(options.dir + "/*.txt")
    print("Count reports loaded: {count}".format(count=data.count()))

    # Split the documents in tokens and keep only those that are meaningful words
    docs = data.map(lambda doc: (doc[0], re.split("[\s;,#]", eliminated_repeated_chars(doc[1])))) \
        .map(lambda doc: (doc[0], [word for word in doc[1] if re.match("^[a-zA-Z][\w&\-]*[a-zA-Z]$", word)])) \
        .map(lambda doc: (doc[0], [word for word in doc[1] if not (word in STOPWORDS)])) \
        .map(lambda doc: (doc[0], [word for word in doc[1] if len(word) > 2])) \
        .toDF(["id", "tokens"])

    # Calculates the tf-idf score
    vectorizerModel = CountVectorizer(inputCol="tokens", outputCol="rawFeatures", minTF=1.0, minDF=1.0).fit(docs)
    vectorizer = vectorizerModel.transform(docs)
    idf = IDF(inputCol='rawFeatures', outputCol='features', minDocFreq=1)
    idfModel = idf.fit(vectorizer)
    tfidf = idfModel.transform(vectorizer)

    vocab = vectorizerModel.vocabulary

    ont = utils.read_ontology(options.ontology)

    # Analyze the documents and extract the meaningful information
    docs = tfidf.select("id", "features") \
        .map(lambda doc: analyse(doc, vocab, ont)) \
        .collect()

    # Serialize the output
    with open(options.dirout + "topics.pickle", "w") as fout:
        cPickle.dump(docs, fout)
