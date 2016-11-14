from privateclean.loaders.csv_loader import CSVLoader
from privateclean.relations import Relation, PrivateRelation, CleanPrivateRelation

"""
Provider
"""

#Load a dataset using the CSVLoader
c = CSVLoader()
loadedData = c.loadFile('datasets/adult.data')

#Initialize a private relation, stores it to a file
p = Relation(loadedData)
p.makePrivate(p=0.25, b=1, file="private.bin")



"""
Analyst
"""
#initializes the private relation
pr = PrivateRelation(file="private.bin")

print pr.sum(0,1, lambda x: x == ' Self-emp-inc')

c = CleanPrivateRelation(pr)

c.addMap(1, ' Self-emp-inc', 'Self-emp-inc')

print c.sum(0,1, lambda x: x == 'Self-emp-inc')

c.addMap(1, ' Private', 'Self-emp-inc')

print c.sum(0,1, lambda x: x == 'Self-emp-inc')






