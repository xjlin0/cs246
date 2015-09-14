 #CS100.1x lab3_text_analysis_and_entity_resolution_student
 #Part 3: ER as Text Similarity - Cosine Similarity
def dotprod(a, b):
""" Compute dot product Args:
a (dictionary): first dictionary of record to value
b (dictionary): second dictionary of record to value Returns:
dotProd: result of the dot product with the two input dicti onaries
"""
return sum(value*b.get(key, 0) for key, value in a.items())
def norm(a):
""" Compute square root of the dot product Args:
a (dictionary): a dictionary of record to value Returns:
norm: a dictionary of tokens to its TF values """
return math.sqrt(sum([value*value for key, value in a.item s()]))
def cossim(a, b):
""" Compute cosine similarity Args:
a (dictionary): first dictionary of record to value
b (dictionary): second dictionary of record to value Returns:
cossim: dot product of two dictionaries divided by the norm of the first dictionary and
"""
then by the norm of the second dictionary
return (dotprod(a, b)/norm(a))/norm(b) testVec1 = {'foo': 2, 'bar': 3, 'baz': 5 }
testVec2 = {'foo': 1, 'bar': 0, 'baz': 20 }
dp = dotprod(testVec1, testVec2) nm = norm(testVec1)
print dp, nm



#TODO:Replace<FILLIN>withappropriatecode
def cosineSimilarity(string1, string2, idfsDictionary):
""" Compute cosine similarity between two strings Args:
string1 (str): first string
string2 (str): second string
idfsDictionary (dictionary): a dictionary of IDF values
Returns:
cossim: cosine similarity value
"""
w1 = tfidf(tokenize(string1),idfsDictionary) w2 = tfidf(tokenize(string2),idfsDictionary) return cossim(w1, w2)
cossimAdobe = cosineSimilarity('Adobe Photoshop', 'Adobe Illustrator',
idfsSmallWeights)
print cossimAdobe