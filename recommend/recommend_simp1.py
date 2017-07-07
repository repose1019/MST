# -*-coding:utf-8-*- 
# @author: 'Repose' 
# @date: 2017/6/30


# from sklearn.model_selection import cross_val_score
# from sklearn.datasets import make_blobs
# from sklearn.ensemble import RandomForestClassifier
# from sklearn.ensemble import ExtraTreesClassifier
# from sklearn.tree import DecisionTreeClassifier
#
# X, y = make_blobs(n_samples=10000, n_features=10, centers=100,
#     random_state=0)
#
# clf = DecisionTreeClassifier(max_depth=None, min_samples_split=2,
#     random_state=0)
# scores = cross_val_score(clf, X, y)
# print(scores.mean())


from numpy import *
from numpy import linalg as la  # 用到别名


# 这里主要结合推荐系统介绍SVD，所以这里的数据都可以看成是用户对物品的一个打分
def loadExData():
    return asanyarray([[0, 0, 0, 2, 2],
            [0, 0, 0, 3, 3],
            [0, 0, 0, 1, 1],
            [1, 1, 1, 0, 0],
            [2, 2, 2, 0, 0],
            [5, 5, 5, 0, 0],
            [1, 1, 1, 0, 0]])


def loadExData2():
    return asanyarray([[0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 5],
            [0, 0, 0, 3, 0, 4, 0, 0, 0, 0, 3],
            [0, 0, 0, 0, 4, 0, 0, 1, 0, 4, 0],
            [3, 3, 4, 0, 0, 0, 0, 2, 2, 0, 0],
            [5, 4, 5, 0, 0, 0, 0, 5, 5, 0, 0],
            [0, 0, 0, 0, 5, 0, 1, 0, 0, 5, 0],
            [4, 3, 4, 0, 0, 0, 0, 5, 5, 0, 1],
            [0, 0, 0, 4, 0, 4, 0, 0, 0, 0, 4],
            [0, 0, 0, 2, 0, 2, 5, 0, 0, 1, 2],
            [0, 0, 0, 0, 5, 0, 0, 0, 0, 4, 0],
            [1, 0, 0, 0, 0, 0, 0, 1, 2, 0, 0]])


def ecludSim(inA, inB):
    return 1.0 / (1.0 + la.norm(inA - inB))  # 计算向量的第二范式,相当于直接计算了欧式距离


def pearsSim(inA, inB):
    if len(inA) < 3:
        return 1.0
    else:
        return 0.5 + 0.5 * corrcoef(inA, inB, rowvar=0)[0][1]  # corrcoef直接计算皮尔逊相关系数


def cosSim(inA, inB):
    num = float(inA.T * inB)
    denom = la.norm(inA) * la.norm(inB)
    return 0.5 + 0.5 * (num / denom)  # 计算余弦相似度


# 协同过滤算法
# dataMat 用户数据 user 用户 simMeas 相似度计算方式 item 物品
def standEst(dataMat, user, simMeas, item):
    n = shape(dataMat)[1]  # 计算列的数量，物品的数量
    simTotal = 0.0
    ratSimTotal = 0.0
    for j in range(n):
        userRating = dataMat[user, j]
        print(dataMat[user, j])
        if userRating == 0: continue  # 如果用户u没有对物品j进行打分，那么这个判断就可以跳过了
        overLap = nonzero(logical_and(dataMat[:, item].A > 0,
                                      dataMat[:, j].A > 0))[0]  # 找到对物品 j 和item都打过分的用户
        if len(overLap) == 0:
            similarity = 0
        else:
            similarity = simMeas(dataMat[overLap, item], dataMat[overLap, j])  # 利用相似度计算两个物品之间的相似度

        print('the %d and %d similarity is: %f' % (item, j, similarity))
        simTotal += similarity
        ratSimTotal += similarity * userRating  # 待推荐物品与用户打过分的物品之间的相似度*用户对物品的打分
    if simTotal == 0:
        return 0
    else:
        return ratSimTotal / simTotal


# 利用SVD进行分解，但是这里是直接用的库里面的函数
# 如果自己实现一个SVD分解，我想就是和矩阵论里面的求解知识是一样的吧，但是可能在求特征值的过程中会比较痛苦
def svdEst(dataMat, user, simMeas, item):
    n = shape(dataMat)[0]
    simTotal = 0.0
    ratSimTotal = 0.0
    U, Sigma, VT = la.svd(dataMat)  # 直接进行分解
    Sig4 = mat(eye(4) * Sigma[:4])  # arrange Sig4 into a diagonal matrix
    xformedItems = dataMat.T * U[:, :4] * Sig4.I  # create transformed items
    for j in range(n):
        userRating = dataMat[user, j]
        if userRating == 0 or j == item: continue
        similarity = simMeas(xformedItems[item, :].T,
                             xformedItems[j, :].T)
        print('the %d and %d similarity is: %f' % (item, j, similarity))
        simTotal += similarity
        ratSimTotal += similarity * userRating
    if simTotal == 0:
        return 0
    else:
        return ratSimTotal / simTotal


# 真正的推荐函数，后面两个函数就是采用的相似度的计算方法和推荐用的方法
def recommend(dataMat, user, N=3, simMeas=cosSim, estMethod=standEst):
    unratedItems = nonzero(dataMat[user, :].A == 0)[1]
    # find unrated items  nonzero()[1]返回的是非零值所在的行数，返回的是一个元组   if len(unratedItems) == 0: return 'you rated everything'
    itemScores = []
    for item in unratedItems:
        estimatedScore = estMethod(dataMat, user, simMeas, item)
        itemScores.append((item, estimatedScore))
    return sorted(itemScores, key=lambda jj: jj[1], reverse=True)[:N]


# 扩展的例子，利用SVD进行图像的压缩
# 将图像打印出来
def printMat(inMat, thresh=0.8):
    for i in range(32):
        for k in range(32):
            if float(inMat[i, k]) > thresh:
                print(1,)
            else:
                print(0,)

# 最后发现重构出来的数据图是差不多的
def imgCompress(numSV=3, thresh=0.8):
    myl = []
    for line in open('0_5.txt').readlines():
        newRow = []
        for i in range(32):
            newRow.append(int(line[i]))
        myl.append(newRow)
    myMat = mat(myl)  # 将数据读入了myMat当中

    print("****original matrix******")
    printMat(myMat, thresh)
    U, Sigma, VT = la.svd(myMat)
    SigRecon = mat(zeros((numSV, numSV)))  # 构建一个3*3的空矩阵
    for k in range(numSV):  # construct diagonal matrix from vector
        SigRecon[k, k] = Sigma[k]
    reconMat = U[:, :numSV] * SigRecon * VT[:numSV, :]
    print("****reconstructed matrix using %d singular values******" % numSV)
    printMat(reconMat, thresh)


if __name__ == '__main__':
    ecludSim(loadExData(),loadExData2())