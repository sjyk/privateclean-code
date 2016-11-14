"""
This class implements the basic relational and query processing structure
"""
from privateclean.loaders.type_inference import LoLTypeInference
import numpy as np
import copy
import pickle


class Relation(object):
    """
    This class acts as a wrapper for a relation, a relation contains 
    a list of lists. Right now we only support categorical and numerical 
    attribute
    """

    def __init__(self, original_data):
        """
        orginal_data is a list of lists
        """
        self.original_data = original_data
        self.types = LoLTypeInference().getDataTypes(original_data)

        if 'string' in self.types:
            raise ValueError("String Valued Data is Not Supported in PrivateClean")

        self.domains, self.domain_array = self.gatherDomains()



    """
    This function generates the private relation and
    stores it to a file
    """
    def makePrivate(self, p, b, file='private.bin'):

        private_relation = copy.copy(self.original_data)

        for datum in private_relation:
            for i,t in enumerate(self.types):

                if t =='numerical':
                    datum[i] = str(float(datum[i]) + np.random.laplace(scale=b))
                elif np.random.rand(1,1) < p:
                    tindex = len(self.domain_array[i])
                    index = np.random.choice(np.arange(tindex))
                    datum[i] = self.domain_array[i][index]

        output = {'data': private_relation, 
                  'params': (p,b), 
                  'domains': self.domain_array,
                  'types': self.types}

        pickle.dump(output, open(file,'wb'))


    """
    Helper method to calculate the domain 
    of each of the attributes
    """
    def gatherDomains(self):

        D = len(self.types)

        domains = [set()]*D

        for datum in self.original_data:
            for i in range(D):
                domains[i].add(datum[i])

        domain_array = [list(d) for d in domains]

        return domains, domain_array





"""
Once you store a private relation to a file
you can load it using this class.
"""
class PrivateRelation(object):

    """
    Initialized with a path to a private table
    """
    def __init__(self, file="private.bin"):
        data = pickle.load(open(file,'rb'))
        self.private_data = data['data']
        self.types = data['types']
        self.domains = data['domains']
        self.p = data['params'][0]
        self.b = data['params'][1]


    """
    Count Query It has the semantics of a
    SQL count query, provide column number
    and a predicate as a lambda function
    """
    def count(self, col, pred):

        if self.types[col] != 'categorical':
            raise ValueError("This type of predicate is not yet supported")

        l = float(len([i for i in self.domains[col] if pred(i)]))
        N = len(self.domains[col])
        S = len(self.private_data)
        cp = float(len([row for row in self.private_data if pred(row[col])]))
        tn = self.p*l/N
        tp = (1-self.p) + self.p*l/N

        return (cp - S*tn)/(tp - tn)

    """
    SUM query. It has the semantics of an SQL sum query.
    Specify the aggregation attribute and the predicate column
    and a predicate as a lambda function
    """
    def sum(self, scol , pcol, pred):

        if self.types[pcol] != 'categorical':
            raise ValueError("This type of predicate is not yet supported")

        if self.types[scol] != 'numerical':
            raise ValueError("Must sum over a numerical value")

        l = float(len([i for i in self.domains[pcol] if pred(i)]))
        N = len(self.domains[pcol])
        S = len(self.private_data)
        tn = self.p*l/N
        tp = (1-self.p) + self.p*l/N

        hp = np.sum([float(row[scol]) for row in self.private_data if pred(row[pcol])])
        hc = np.sum([float(row[scol]) for row in self.private_data if not pred(row[pcol])])

        return ((1-tn)*hp - tn*hc)/(tp - tn)


    """
    AVG query. It has the semantics of an SQL AVG query.
    Specify the aggregation attribute and the predicate column
    and a predicate as a lambda function
    """
    def average(self, scol, pcol, pred):
        return self.sum(scol,pcol,pred)/self.count(pcol,pred)





"""
This class implements the cleaning function and
maintains the bi-partite graph.
"""
class CleanPrivateRelation(object):

    """
    This takes as input a private relation
    """
    def __init__(self, private_relation):
        self.private_relation = private_relation
        self.cleaner = [{}]*len(private_relation.types) #graph


    """
    If you apply some cleaning it will add it 
    to the graph
    """
    def addMap(self, col, dirty, clean):
        self.cleaner[col][clean] = dirty


    """
    Re-writes the predicate with the graph
    """
    def translatePredicate(self, col, pred):
        dirty_domain = set([self.cleaner[col][k] for k in self.cleaner[col] if pred(k)])
        return lambda x: x in dirty_domain

    #Passes queries through with the re-written predicates
    
    def count(self, col, pred):
        tpred = self.translatePredicate(col, pred)
        return self.private_relation.count(col, tpred)

    def sum(self, scol, pcol, pred):
        tpred = self.translatePredicate(pcol, pred)
        return self.private_relation.sum(scol, pcol, tpred)

    def average(self, scol, pcol, pred):
        tpred = self.translatePredicate(pcol, pred)
        return self.private_relation.average(scol, pcol, tpred)












