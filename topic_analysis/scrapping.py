import io
import optparse
import sys
from HTMLParser import HTMLParser

import os
import re
from BeautifulSoup import BeautifulSoup
from os import listdir
from os.path import isfile, join


def get_page(f_url):
    with open(f_url, "r") as f_in:
        return f_in.read()


def visible(element):
    if element.parent.name in ['style', 'script', '[document]', 'head', 'title']:
        return False
    elif re.match('<!--.*-->', str(element)):
        return False
    return True


def scrapping(inputfile, outputfile):
    """
    Scrapping the html file
    :param inputfile: the input file
    :param outputfile: the output file
    :return:
    """
    p_digits = re.compile("\\d*")
    p_words = re.compile("[a-zA-Z]")
    p_par = re.compile("[()]")
    data = BeautifulSoup(get_page(inputfile))
    h = HTMLParser()

    texts = data.findAll(text=True)
    visible_texts = filter(visible, texts)
    with io.open(outputfile, "w", encoding="utf-8") as f_out:
        for text in visible_texts:
            t = h.unescape(text).replace("\n", " "). replace(u'\xa0', " ").strip()
            t = p_digits.sub("", t)
            t = p_par.sub("", t)

            if len(t) > 0 and p_words.search(t):
                f_out.write(u"\n")

                f_out.write(t)


if __name__=="__main__":
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

    if not hasattr(options, "input") or options.input is None:
        sys.exit("You must enter the input file.")

    if not hasattr(options, "dirout") or options.dirout is None:
        sys.exit("You must enter the output directory path.")

    if not options.dirout.endswith("/"):
        options.dirout += "/"

    if not os.path.exists(options.dirout):
        os.makedirs(options.dirout)

    files = [f for f in listdir(options.dir) if isfile(join(options.dir, f))]

    for f in files:
        scrapping(options.dir + f, options.dirout + f + ".txt")
