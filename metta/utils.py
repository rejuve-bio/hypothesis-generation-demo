from sklearn import metrics
import numpy as np
from sklearn.linear_model import LogisticRegression

def auc(data, i):
    arr = np.array(data)
    y_true, y_score = arr[:,0], arr[:,i]
    # print(f"y_true:\n{y_true}")
    # print(f"y_score:\n{y_score}")
    fpr, tpr, thresholds = metrics.roc_curve(y_true, y_score, pos_label=1)
    auc_score = metrics.auc(fpr, tpr)
    return auc_score

def auc_log_reg(train, test):
    train_arr = np.array(train)
    test_arr = np.array(test)
    print(f"Dim.Train: {train_arr.shape}")
    y_train, X_train = train_arr[:,0], train_arr[:,1:]
    y_test, X_test = test_arr[:,0], test_arr[:,1:]
    model = LogisticRegression()
    model.fit(X_train, y_train)
    y_pred = model.predict_proba(X_test)[:,1]
    for name, coef in zip(["strength", "conf"], model.coef_[0]):
        print(f"{name}: {coef:.4f}")
    fpr, tpr, thresholds = metrics.roc_curve(y_test, y_pred, pos_label=1)
    auc_score = metrics.auc(fpr, tpr)
    return auc_score

def mean_and_se(scores):
    n = len(scores)
    if n == 0:
        return [0.0, 0.0]
    
    mean = np.mean(scores)
    # ddof=1 calculates sample standard deviation
    std = np.std(scores, ddof=1)
    
    return [float(mean), float(std)]