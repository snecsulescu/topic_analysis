import re


def read_ontology(input):
    """
    Read the given ontology
    :param input: the inpput path
    :return: a dictionaty {topic: [list_of_terms]}
    """
    with open(input) as data_file:
        n_brackets = 0

        terms = dict()

        for row in data_file:
            n_brackets += row.count('{')
            n_brackets -= row.count('}')

            if '"topic_cd"' in row:
                res = re.search('"topic_cd"\s*:\s*"([\w\s\-&]+)",', row)
                if res is not None:
                    topic_cd = res.group(1).lower()

            if '"term"' in row:
                res = re.search('"term"\s*:\s*"([\w\s\-&]+)",', row)
                if res is not None:
                    term = res.group(1).lower().replace(" ", "_")

            if n_brackets == 0 and topic_cd is not None and term is not None:
                if not (topic_cd in terms):
                    terms[topic_cd] = []
                terms[topic_cd].append(term)

    return terms


def eliminated_repeated_chars(text):
    """ Eliminates all the characters '_', '-' or '&' that occur repeated more than once.
    :param text: input text
    :return: modified text
    """
    text = re.sub('_{2,}', '', text)
    text = re.sub('-{2,}', '', text)
    text = re.sub('&{2,}', '', text)
    return text

