#use .round() for prediction accuracy_score is a classification metric, you cannot use it for a regression problem.


    
import sys
from time import time
sys.path.append("../tools/")
from email_preprocess import preprocess


### features_train and features_test are the features for the training
### and testing datasets, respectively
### labels_train and labels_test are the corresponding item labels
features_train, features_test, labels_train, labels_test = preprocess()

from sklearn import tree
clf = tree.DecisionTreeRegressor(min_samples_split=40)
clf.fit(features_train, labels_train)
pred = clf.predict(features_test)
from sklearn.metrics import accuracy_score

print("Accuracy-->",accuracy_score(pred.round(),labels_test))
#########################################################
### your code goes here ###
print("No. of features data",len(features_train[0]))

#########################################################


