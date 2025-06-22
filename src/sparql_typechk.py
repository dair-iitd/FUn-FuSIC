import copy
import re
import sys
import time
import logging

import urllib

from SPARQLWrapper import SPARQLWrapper, JSON
sys.path.append("/Users/riyasawhney/Desktop/kbqa")
sys.path.append("/Users/riyasawhney/Desktop/kbqa/dataset_analysis/graph_sample")
import json

from typing import List, Dict, Tuple

HEADER = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> 
PREFIX : <http://rdf.freebase.com/ns/> 
"""

sparql = SPARQLWrapper("http://10.237.23.185:3001/sparql")
sparql.setTimeout(200)
sparql.setReturnFormat(JSON)

def execute_query(query: str, cnt: int=0) -> List[str]:
    #TODO: Strict checking of void variables
    if(query == 'error'):
        logging.info('youre trying to execute an error! pls recheck')
        return []
    query= query.strip()
    if not query.startswith('PREFIX'):
        query = HEADER + query
    sparql.setQuery(query)
    try:
        results = sparql.query().convert()
        return results
    except urllib.error.URLError as e:
        logging.info(f'oof! error encontered: {e}')
        logging.info(query)
        # exit(0)
        return []
    except TimeoutError:
        logging.info('oof! time limit exceeded')
        logging.info(query)
        return []
    except Exception as e:
        if cnt>5:
            return []
        logging.info(f'oof! error encontered: {e}')
        logging.info(f'count: {cnt}')
        logging.info(query)
        return []

MAGENTA = "\033[35m"
RESET = "\033[0m"

def get_vars(sparql: str) -> list:
    """
    it returns a list of all variables in the sparql
    """
    vars = []
    words = sparql.split()
    for word in words:
        if word.startswith('?'):
            vars.append(word)
    vars = list(set(vars))
    return (vars)

def ensure_min_max_operands_are_literals(query):
    """
    Regular expression to match MIN or MAX functions. 
    It captures the variable used as an operand for MIN or MAX
    """
    # logging.info('query: ', query)
    min_max_pattern = r'(MIN|MAX)\s*\(\s*(\?[a-zA-Z0-9]+)\s*\)\s*AS\s*(\?[a-zA-Z0-9]+)\s*\)\s*WHERE\s*\{'

    # This function will be called for each match
    def add_filter(match):
        function = match.group(1)
        variable = match.group(2)
        minmaxval = match.group(3)

        # Reconstruct the expression with the added FILTER clause
        return f'{function}({variable}) AS {minmaxval}) WHERE {{\nFILTER (isLiteral({variable}))'


    # Add filter clauses after MIN or MAX functions
    updated_query = re.sub(min_max_pattern, add_filter, query)

    return updated_query

def last_bracket(words: list) -> str:
    """
    it returns the last bracket in words
    """
    for word in reversed(words):
        if word == '(' or word == ')' or word == '}' or word == '{':
            return word
    return None

def next_bracket(words: list) -> str:
    """
    it returns the next bracket in words
    """
    for word in words:
        if word == '(' or word == ')' or word == '}' or word == '{':
            return word
    return None


def bracket_select_clause(sparql: str, vars: list) -> str:
    """
    bracketing the initial select clause
    """
    for var in vars:
        pattern = fr"SELECT DISTINCT\s*{re.escape(var)}\s*WHERE"
        replacement = f"SELECT DISTINCT ( {var} ) WHERE"
        sparql = re.sub(pattern, replacement, sparql)
    return sparql

def remove_filter(sparql: str) -> str:
    """
    it removes all the filter clauses from the sparql
    """
    pattern = r"FILTER\s*\(\s*isLiteral\s*\(\s*\?[a-zA-Z0-9]+\s*\)\s*\)"
    sparql = re.sub(pattern, '', sparql)
    return sparql

def decouple_var(sparql: str, var: str) -> str:
    """
    it replaces distinct occurences of var (eg: ?x)
    (outside of 'select' clauses) with ?x, ?x_ri1, ?x_ri2...
    """
    new_sparql = ''
    words = sparql.split()
    ctr = 0
    for words_idx, word in enumerate(words):
        if word== var:
            # we check if it is inside ()
            # if yes, then we don't replace it
            if last_bracket(words[:words_idx]) == '(' and next_bracket(words[words_idx:]) == ')':
                new_sparql += ' ' + word
            elif ctr == 0:
                new_sparql += ' ' + word 
                ctr +=1
            else:
                new_sparql += ' ' + word + '_ri' + str(ctr)
                ctr += 1
        else:
            new_sparql += ' ' + word

    return new_sparql

def simplify_sparql(sparql: str) -> str:
    """
    it simplifies the sparql by removing all the
    triple patterns that are not needed for the query
    """
    sparql = sparql.replace(' . ', ' .\n')
    sparql = sparql.replace('{', '{\n')
    sparql = sparql.replace('}', '}\n')
    lines = sparql.split('\n')
    new_lines = []
    for line in lines:
        if line.endswith('.'):
            # if all vars in line end in _ri1, _ri2, ... then delete this line
            vars = get_vars(line)
            if all([var.endswith('_ri1') or var.endswith('_ri2') or var.endswith('_ri3') or var.endswith('_ri4') for var in vars]):
                continue
            else:
                new_lines.append(line)

        else:
            new_lines.append(line)

    new_sparql = '\n'.join(new_lines)
    return new_sparql
def find_closing_bracket(sparql: str) -> int:
    """
    it finds the position of the closing bracket
    corresponding to the opening bracket at the beginning
    of sparql
    """
    ctr = 0
    for words_idx, word in enumerate(sparql):
        if word == '{':
            ctr += 1
        elif word == '}':
            ctr -= 1
            if ctr == -1:
                return words_idx
    return None


def simplify_min_clause(sparql: str) -> str:
    """
    `SELECT (MIN(?var1) as ?var2) WHERE {...}`
    transformed to 
    `SELECT (?var1 as ?var2) WHERE {...} LIMIT 1`
    
    **Note**: works for MAX as well
    """
    # step 1: find the min/max clause
    min_max_pattern = r'(MIN|MAX)\s*\(\s*(\?[a-zA-Z0-9]+)\s*\)\s*AS\s*(\?[a-zA-Z0-9]+)\s*\)\s*WHERE\s*\{'
    match = re.search(min_max_pattern, sparql)
    if match is None:
        return sparql
    else:
        function = match.group(1)
        variable = match.group(2)
        minmaxval = match.group(3)
        # step 2: replace the min/max clause with a select clause
        sparql = sparql.replace(match.group(0), f'{variable} AS {minmaxval}) WHERE {{')
        # step 3: if it exists, find the corresponding closing }
        beg_sparql = sparql.split(f'SELECT ( {variable} AS {minmaxval}) WHERE {{')[0]
        rem_sparql = sparql.split(f'SELECT ( {variable} AS {minmaxval}) WHERE {{')[1]
        closing_pos = find_closing_bracket(rem_sparql)
        # step 4: add a limit clause
        sparql = beg_sparql + f'SELECT ({variable} AS {minmaxval}) WHERE {{' + rem_sparql[:closing_pos + 1] + f'LIMIT 1\n' + rem_sparql[closing_pos + 1:]
        return sparql

def check_types_are_compatible(types: Dict[str, List[str]], metadata: Dict[str, List[str]]) -> str:
    """
    check if the types are compatible for each variable
    for entity, see if it matches all types assigned to it. 
    """
    for var in types:
        logging.info(var)
        logging.info(types)
        var_types = list(set(types[var]))
        if len(var_types) == 1 and ('.' not in var):
            continue
        else:
            var_types = list(set(var_types))
            import utils
            query = 'PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>  \
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \
PREFIX : <http://rdf.freebase.com/ns/> ASK WHERE {'
            if '.' in var:
                query += f'VALUES ?x {{{var}}}'
            for i in range(len(var_types)-1):
                if var_types[i] != 'type.float':
                    query += f' ?x rdf:type :{var_types[i]} .'
            if var_types[-1] != 'type.float':
                query += f' ?x rdf:type :{var_types[-1]} }}'
            else:
                query += '}'
            logging.info(query)
            logging.info(utils.execute_query(query, -1))
            res = utils.execute_query(query, -1)['boolean']
            logging.info(res)
            if not res:
                feedback = 'warning: The types of relations don\'t match for variable ' + var + f' in the query. The assigned relation types by {metadata[var]} are {types[var]}. These types are mutually incompatible.'
                logging.info('returing feedback...'+feedback)
                return feedback
            if 'type.float' in var_types and '.' in var and 'float' not in var:
                feedback = 'warning: The types of relations don\'t match for variable ' + var + f' in the query. The assigned relation types by {metadata[var]} are {types[var]}. These types are mutually incompatible.'
                logging.info('returing feedback...'+feedback)
                return feedback
    logging.info('okok')      
    logging.info('returing feedback...correct')      
    return 'correct'
    



RELATION_TYPES = {}
with open('../data/freebase/fb_roles.txt') as f:
    lines = f.readlines()
    for line in lines:
        words = line.split()
        RELATION_TYPES[words[1]] = [words[0], words[2]]
def get_types_for_var(sparql: str) -> Tuple[Dict[str, List[str]], Dict[str, List[str]]]:
    """
    for each sentence in the sparql query, get the types assoicated using the rels. 
    for ?s :r ?o, type of ?s is given by RELATION_TYPES[r][0] and type of ?o is given by RELATION_TYPES[r][1]
    return a dict {"?s": [type1, type2], "?o": [type3, type4]..}
    """
    logging.info('getting types ...')
    types = {}
    metadata = {} # specifies the relations from which the constraint originated
    sparql = sparql.replace('ns:', ':')
    sparql = sparql.replace(' . ', ' .\n')
    sparql = sparql.replace('{', '{\n')
    sparql = sparql.replace('}', '}\n')
    lines = sparql.split('\n')
    for line in lines:
        if line.endswith('.'):
            line = line.strip()

            words = line.split()
            logging.info(words)
            if len(words) < 3:
                continue
            words[1] = words[1][1:]
            if words[1] in RELATION_TYPES and not('type.object.type' in words[1] or 'rdf:type' in words[1]):
                if words[0] not in types:
                    types[words[0]] = [RELATION_TYPES[words[1]][0]]
                    metadata[words[0]] = [words[1]]
                else:
                    types[words[0]].append(RELATION_TYPES[words[1]][0])
                    metadata[words[0]].append(words[1])
                if words[2] not in types:
                    types[words[2]] = [RELATION_TYPES[words[1]][1]]
                    metadata[words[2]] = [words[1]]
                else:
                    types[words[2]].append(RELATION_TYPES[words[1]][1])
                    metadata[words[2]].append(words[1])
            elif 'type.object.type' in words[1] or 'rdf:type' in words[1]:
                if words[0] not in types:
                    types[words[0]] = [words[2][1:]]
                    metadata[words[0]] = [words[1]+ ' '+ words[2][1:]]
                else:
                    types[words[0]].append(words[2][1:])
                    metadata[words[0]].append(words[1]+ ' '+ words[2][1:])
    logging.info(f'types: {types}')
    logging.info(f'metadata: {metadata}')
    return types, metadata

def check_type(sparql: str) -> str:
    """
    ### checks whether the query is semantically meaningful
    for each variable x, we decouple all other variables 
    and check whether we have a non empty answer
    
    Note: a non empty answer assets type correctness
    but an empty answer may still mean type is correct
    although this is somewhat less likely

    if a variable occurs as arg of MIN/MAX, we check
    if it is a literal. 
    """
    sparql = sparql.replace('(', ' ( ')
    sparql = sparql.replace(')', ' ) ')
    sparql = sparql.replace('COUNT', '') # a count query is type-correct if its non-COUNT version is. 
    sparql = sparql.replace('ns:', ':')
    feedback = 'correct'
    vars = get_vars(sparql)
    sparql = bracket_select_clause(sparql, vars)
    sparql = remove_filter(sparql)
    types, metadata = get_types_for_var(sparql)
    feedback = check_types_are_compatible(types, metadata)
    
    if feedback == 'correct':
        # check if a variable occurs as arg of MIN/MAX
        # if yes, check if it is a literal
        # if not, then type is incorrect 
        """
        select( MIN(?x) as ?y) WHERE
        {
            isLiteral(?x)
            ?x :father_of ?z . 
        }
        """
        min_max_literals_query = ensure_min_max_operands_are_literals(sparql)
        min_max_literals_query = min_max_literals_query.replace ('DISTINCT', '')
        # logging.info('sparql after ensuring min/max operands are literals: ', min_max_literals_query)
        try:
            new_ans = execute_query(min_max_literals_query + ' LIMIT 1')
            old_ans = execute_query(sparql)
            if new_ans == []:
                new_ans = {'results': {'bindings': ['random_stuff']}}
            if old_ans == []:
                old_ans = {'results': {'bindings': ['random_stuff']}}
        except:
            # import ipdb; ipdb.set_trace()
            logging.info(f'erroring out: {min_max_literals_query} LIMIT 1')
            logging.info('query timed out..probably lots of results :)')
            new_ans = {'results': {'bindings': ['random_stuff']}}
            old_ans = {'results': {'bindings': ['random_stuff']}}
        logging.info(min_max_literals_query + ' LIMIT 1')
        logging.info(new_ans)
        if (len(new_ans['results']['bindings']) == 0) and not (len(old_ans['results']['bindings']) == 0):
            feedback = 'warning: The types of relations and entities don\'t match in the query'
            logging.info(feedback)
    return feedback



if __name__ == '__main__':
    import ipdb; ipdb.set_trace()
