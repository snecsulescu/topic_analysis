import optparse
import sys

from pyspark.context import SparkContext

import codecs
import os
from topic_analysis import utils

sc = SparkContext()


def write(dirout, filename, text):
    print filename

    filename = dirout + filename[5:].rsplit("/", 1)[1]
    with codecs.open(filename, "w", encoding="utf-8") as fout:
        fout.write(unicode(text).encode('utf-8').decode('utf-8'))


def search_terms(doc_text):
    for topic in ont:
        for term in ont[topic]:
            if term in doc_text:
                print term
            doc_text = doc_text.replace(term, term.replace(" ", "_"))

    return doc_text


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

    if not hasattr(options, "dir"):
        sys.exit("You must enter the main directory path where the reports can be found.")

    if not hasattr(options, "dirout"):
        sys.exit("You must enter the directory path where the tokenized reports will be writen.")

    if not hasattr(options, "ontology") or options.ontology is None:
        sys.exit("You must enter the ontology file.")

    if not options.dir.endswith("/"):
        options.dir += "/"

    if not options.dirout.endswith("/"):
        options.dirout += "/"

    if not os.path.exists(options.dirout):
        os.makedirs(options.dirout)

    ont = utils.read_ontology(options.ontology)

    # Load and parse the data
    data = sc.wholeTextFiles(options.dir + "/*.txt")
    # print(data.count())

    tf = {}
    df = {}
    docs = data.map(lambda doc: (doc[0], doc[1].strip().lower())) \
        .map(lambda doc: (doc[0], search_terms(doc[1]))) \
        .foreach(lambda doc: write(options.dirout, doc[0], doc[1]))
