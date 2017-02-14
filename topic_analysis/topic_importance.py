import cPickle
import operator
import optparse
import sys

import os

if __name__ == "__main__":
    parser = optparse.OptionParser()
    parser.add_option('-i', '--input',
                      dest="input",
                      type="string",
                      help="The input file."
                      )
    parser.add_option('-o', '--dirout',
                      dest="dirout",
                      type="string",
                      help="The output directory."
                      )
    options, remainder = parser.parse_args()

    if not hasattr(options, "input") or options.input is None:
        sys.exit("You must enter the input file.")

    if not hasattr(options, "dirout") or options.dirout is None:
        sys.exit("You must enter the output directory path.")

    if not options.dirout.endswith("/"):
        options.dirout += "/"

    if not os.path.exists(options.dirout):
        os.makedirs(options.dirout)

    options, remainder = parser.parse_args()

    # Open the serialized file
    with open(options.input, "r") as fin:
        docs = cPickle.load(fin)

    # Write the Tf-Idf weights for each report.
    with open(options.dirout + "topics_per_file.txt", "w") as fout:
        topic_group = {}
        for doc_name, weight, weight_ont, terms_ont in docs:
            if len(weight_ont) > 0:

                # Write the report name and the weight, the weight is informative to compare the topic weight with
                #  the file weight
                fout.write("{file}\t{weight}\n".format(file=doc_name, weight=weight))
                sum_ont_sorted = sorted(weight_ont.items(), key=operator.itemgetter(1), reverse=True)

                # Sum the topic weights of each file
                for topic, weight in sum_ont_sorted:
                    fout.write("\t{topic}\t{weight}\n".format(topic=topic, weight=weight))
                    if not (topic in topic_group):
                        topic_group[topic] = 0.0
                    topic_group[topic] += weight

    # Write the Tf-Idf weights for all the reports.
    with open(options.dirout + "topics_per_group.txt", "w") as fout:
        topic_group_sorted = sorted(topic_group.items(), key=operator.itemgetter(1), reverse=True)
        for topic, weight in topic_group_sorted:
            fout.write("{topic}\t{weight}\n".format(topic=topic, weight=weight))

    with open(options.dirout + "topics.txt", "w") as fout:
        topic_group = {}
        for doc_name, weight, weight_ont, terms_ont in docs:
            if len(weight_ont) > 0:

                # Write the report name and the weight, the weight is informative to compare the topic weight with
                #  the file weight
                fout.write("{file}\t{weight}\n".format(file=doc_name, weight=weight))
                sum_ont_sorted = sorted(weight_ont.items(), key=operator.itemgetter(1), reverse=True)

                # Sum the topic weights of each file
                for topic, weight in sum_ont_sorted:
                    fout.write("\t{topic}\t{weight}\n".format(topic=topic, weight=weight))
                    terms_ont_sort = sorted(terms_ont[topic], key=lambda x: x[1], reverse=True)
                    for term, term_weight in terms_ont_sort:
                        fout.write("\t\t{term}\t{weight}\n".format(term=term, weight=term_weight))
