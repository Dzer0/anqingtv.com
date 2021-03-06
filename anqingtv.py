# conding:utf-8
'''
pip install oss2
pip install MySQL-python
'''
import MySQLdb
import oss2
import urlparse
import os
import requests

oss_accesskey = os.getenv('oss_accesskey')
oss_accesskey_secret = os.getenv('oss_accesskey_secret')
oss_bucket_name = os.getenv('oss_bucket_name')

mysqlUser = os.getenv('mysqlUser')
mysqlPwd = os.getenv('mysqlPwd')
mysqlUrl = os.getenv('mysqlUrl')
mysqlPort = os.getenv('mysqlPort')
mysqlDatabasename = os.getenv('mysqlDatabasename')


def Upload_oss_get_url(image_url, id):
    '''
        img_name = images name
        path = Image absolute path + image file name
    '''
    headers = {'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 6.1; \
    en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'}
    # no support china
    if urlparse.urlparse(image_url).query:
        new_url = urlparse.urlparse(image_url).query.split('=')[-1]
        img_name = urlparse.urlparse(new_url).path
        if '.' in img_name:
            img_name = 'oss' + img_name
        else:
            img_name = 'oss' + img_name + '.jpg'
    else:
        new_url = image_url
        img_name = 'oss' + urlparse.urlparse(image_url).path
    endpoint = 'http://oss-cn-hangzhou.aliyuncs.com'
    auth = oss2.Auth(oss_accesskey, oss_accesskey_secret)
    bucket = oss2.Bucket(auth, endpoint, oss_bucket_name)
    # oss2.resumable_upload(bucket, img_name, path)
    input = requests.get(new_url, headers=headers, timeout=5)
    bucket.put_object(img_name, input)
    return img_name


def GetMySQLdbInfo(mysqlUrl, mysqlUser, mysqlPwd, mysqlDatabasename):
    db = MySQLdb.connect(mysqlUrl, mysqlUser, mysqlPwd, mysqlDatabasename)
    cursor = db.cursor()
    # Get old id
    get_old_id_sql = 'select * from history where od=1'
    cursor.execute(get_old_id_sql)
    old_id = cursor.fetchall()[0][0]
    print old_id
    # get new artile id
    get_new_id_sql = 'select * from mac_vod ORDER BY d_id DESC LIMIT 1'
    cursor.execute(get_new_id_sql)
    new_id = cursor.fetchall()[0][0]
    print new_id
    if old_id >= new_id:
        results = False
    else:
        # get all imgurl sql
        # get_all_imgurl_sql = 'select * from mac_vod LIMIT ' + \
        # str(old_id) + ',' + str(new_id)+';'
        get_all_imgurl_sql = 'select * from mac_vod where d_id > %s ' % str(old_id)
        print get_all_imgurl_sql
        cursor.execute(get_all_imgurl_sql)
        results = cursor.fetchall()
        # update old id
        update_old_id_sql = 'update history set old_id=%s where od=1' % new_id
        cursor.execute(update_old_id_sql)
    db.commit()
    db.close()
    return results


def UpdateDataBase(mysqlUrl, mysqlUser, mysqlPwd, mysqlDatabasename, new_img_url, new_d_id):
    db = MySQLdb.connect(mysqlUrl, mysqlUser, mysqlPwd, mysqlDatabasename)
    cursor = db.cursor()
    try:
        # update databse SQL
        sql = 'update mac_vod set d_pic ="%s" where d_id =%s' % (new_img_url, new_d_id)
        print sql
        cursor.execute(sql)
        db.commit()
        db.close()
    except Exception as e:
        print e
    return 'success'


if __name__ == '__main__':
    print 'Python Run Path %s' % os.getcwd()
    results = GetMySQLdbInfo(mysqlUrl, mysqlUser, mysqlPwd, mysqlDatabasename)
    if results:
        for i in results:
            if i[6]:
                print i[0], i[6]
                # DownloadImage_updatedatabse(i[0],i[6])
                try:
                    if 'oss.anqingtv.com' in i[6]:
                        print('%s the updated.') % i[6]
                    else:
                        new_img_url = 'http://oss.anqingtv.com/' + Upload_oss_get_url(i[6], i[0])
                        # new_img_url = 'http://oss.anqingtv.com/oss' + str(urlparse.urlparse(i[6]).path)
                        print new_img_url
                        UpdateDataBase(mysqlUrl, mysqlUser, mysqlPwd, mysqlDatabasename, new_img_url, i[0])
                except Exception as e:
                    print e
            else:
                print 'pic no found'
    else:
        print 'No update moive End!!!'
