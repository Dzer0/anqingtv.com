# conding:utf-8
'''
pip install oss2
pip install MySQL-python
'''
import MySQLdb
import oss2
import urlparse
import os
import urllib2
import requests

oss_accesskey = os.getenv('oss_accesskey')
oss_accesskey_secret = os.getenv('oss_accesskey_secret')
oss_bucket_name = os.getenv('oss_bucket_name')

mysqlUser = os.getenv('mysqlUser')
mysqlPwd = os.getenv('mysqlPwd')
mysqlUrl = os.getenv('mysqlUrl')
mysqlPort = os.getenv('mysqlPort')
mysqlDatabasename = os.getenv('mysqlDatabasename')

def Upload_oss_get_url(image_url,id):
    '''
        img_name = images name
        path = Image absolute path + image file name
    '''
    headers = {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; \
    en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'}
    img_name = 'anqing.com' + urlparse.urlparse(image_url).path
    endpoint = 'http://oss-cn-hangzhou.aliyuncs.com'
    auth = oss2.Auth(oss_accesskey, oss_accesskey_secret)
    bucket = oss2.Bucket(auth, endpoint, oss_bucket_name)
    #oss2.resumable_upload(bucket, img_name, path)
    input = requests.get(image_url ,headers = headers,timeout=5)
    bucket.put_object(img_name, input)
    return True

def GetMySQLdbInfo(mysqlUrl,mysqlUser,mysqlPwd,mysqlDatabasename):
    db = MySQLdb.connect(mysqlUrl,mysqlUser,mysqlPwd,mysqlDatabasename)
    cursor = db.cursor()
    #Get old id
    get_old_id_sql = 'select * from history where od=1'
    cursor.execute(get_old_id_sql)
    old_id = cursor.fetchall()[0][0]
    print old_id
    # get new artile id
    get_new_id_sql = 'select * from mac_vod ORDER BY d_id DESC LIMIT 1'
    cursor.execute(get_new_id_sql)
    new_id = cursor.fetchall()[0][0]
    print new_id
    # get all imgurl sql
    get_all_imgurl_sql = 'select * from mac_vod LIMIT ' + \
    str(old_id) + ',' + str(new_id)+';'
    print get_all_imgurl_sql
    cursor.execute(get_all_imgurl_sql)
    results = cursor.fetchall()
    # update old id
    update_old_id_sql = 'update history set old_id=%s where od=1' %new_id
    cursor.execute(update_old_id_sql)
    db.commit()
    db.close()
    return results

def UpdateDataBase(mysqlUrl,mysqlUser,mysqlPwd,mysqlDatabasename,new_img_url,new_d_id):
    db = MySQLdb.connect(mysqlUrl,mysqlUser,mysqlPwd,mysqlDatabasename)
    cursor = db.cursor()
    try:
        #update databse SQL
        sql = 'update mac_vod set d_pic ="%s" where d_id =%s' %(new_img_url,new_d_id)
        print sql
        cursor.execute(sql)
        db.commit()
        db.close()
    except Exception as e:
        print e
    return 'success'


if __name__ == '__main__':
    print 'Python Run Path %s' % os.getcwd()
    results = GetMySQLdbInfo(mysqlUrl,mysqlUser,mysqlPwd,mysqlDatabasename)
    for i in results:
        if i[6]:
            print i[0],i[6]
            #DownloadImage_updatedatabse(i[0],i[6])
            Upload_oss_get_url(i[6],i[0])
            new_img_url = 'http://oss.zhizhebuyan.com/anqingtv.com' + str(urlparse.urlparse(i[6]).path)
            print new_img_url
            UpdateDataBase(mysqlUrl,mysqlUser,mysqlPwd,mysqlDatabasename,new_img_url,i[0])
        else:
            print 'pic no found'
