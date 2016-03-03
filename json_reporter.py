#!/usr/bin/env python

"""
@charlie sun
@20150209
this is the back end script to generate CDDP report module
"""



"""
BR link: https://wiki.nci.nih.gov/pages/viewpageattachments.action?pageId=253887753&metadataLink=true
"""
#query 1 -- total available files and total available file size
total_sql = """select file_count, total_size
from
(select count(distinct fi.file_id)  as file_count
from %(db)sfile_item fi, %(db)sfile_item_project_study fp
where fi.status = 'Available'
AND   fi.IS_LATEST=1
AND   fi.file_id = fp.file_id
AND   fp.PROJECT_CODE='ER'),
(select sum(file_size) as total_size
from( select distinct fi.file_id, fi.file_size file_size
from %(db)sfile_item fi, %(db)sfile_item_project_study fp
where fi.status = 'Available'
AND   fi.IS_LATEST=1
AND   fi.file_id = fp.file_id
AND   fp.PROJECT_CODE='ER'))
""" % {'db':'MULTIPROJ.'}

#query 2
fcount_by_category_sql = """
SELECT cat_name, count(er_file)
FROM
(SELECT DISTINCT
       fi.file_id as er_file,
       dc.data_category_name as cat_name
FROM %(db)sfile_item fi, %(db)sdata_type d, %(db)sfile_item_project_study fp, %(db)sdata_category dc, %(db)sdata_category_mapping dcm
WHERE fi.status = 'Available'
AND   fi.is_latest = 1
AND   fp.PROJECT_CODE='ER'
AND   fi.file_id = fp.file_id
AND   fi.data_type_id = d.data_type_id
AND   d.data_type_id=dcm.data_type_id
AND   dcm.data_category_id=dc.data_category_id)
GROUP BY cat_name
ORDER BY cat_name
"""  % {'db':'MULTIPROJ.'}


#query3 -- case count by study, also calcualte total cases
case1_sql = """
SELECT study_code,count(participant_id)
FROM
(SELECT DISTINCT
       fp.study_code,
       d.data_type_name,
       p.platform_name,
       u.participant_id
FROM %(db)sfile_item fi, %(db)sdata_type d, %(db)splatform p, %(db)sfile_item_project_study fp, %(db)sfile_item_uuid fu, %(db)suuid u
WHERE fi.status = 'Available'
AND   fi.is_latest = 1
AND   fi.file_id = fp.file_id
AND   fi.file_id = fu.file_id
AND   fu.uuid = u.uuid
AND   fi.data_type_id = d.data_type_id
AND   fi.platform_id = p.platform_id
AND   fp.project_code='ER')
WHERE data_type_name = 'bio'
GROUP BY study_code
ORDER BY study_code
""" % {'db':'MULTIPROJ.'}



#for populating case table
case_table_sql = """
select cid, alt_id, disease, disease_desc, center, data_type, updated_date, count(fid)
from
(SELECT DISTINCT
       u.participant_id as cid,
       u.alt_participant_id as alt_id,
       fp.study_code as disease,
       s.study_name as disease_desc,
       c.display_name as center,
       d.data_type_alias as data_type,
       fi.file_id as fid,
       TO_CHAR(fi.modified_date,'MM/DD/YYYY') as updated_date
FROM MULTIPROJ.file_item fi, MULTIPROJ.data_type d, MULTIPROJ.platform p, MULTIPROJ.file_item_project_study fp, MULTIPROJ.study s, MULTIPROJ.file_item_uuid fu, MULTIPROJ.uuid u, MULTIPROJ.center c
WHERE fi.status = 'Available'
AND   fi.is_latest = 1
AND   fi.file_id = fp.file_id
AND   fp.study_code = s.study_code
AND   fi.file_id = fu.file_id
AND   fu.uuid = u.uuid
AND   fi.center = c.domain_name
AND   fi.data_type_id = d.data_type_id
AND   fi.platform_id = p.platform_id
AND   fp.project_code='ER')
group by alt_id, cid, disease, disease_desc, center, data_type, updated_date
order by alt_id, cid, disease, disease_desc, center, data_type
"""

case_table_sql1 = """
select cid, alt_id, disease, disease_desc, center, data_type, updated_date, count(fid)
from
(SELECT DISTINCT
       u.participant_id as cid,
       u.alt_participant_id as alt_id,
       fp.study_code as disease,
       s.study_name as disease_desc,
       fi.CENTER as center,
       d.data_type_alias as data_type,
       fi.file_id as fid,
       TO_CHAR(fi.modified_date,'MM/DD/YYYY') as updated_date
FROM MULTIPROJ.file_item fi, MULTIPROJ.data_type d, MULTIPROJ.platform p, MULTIPROJ.file_item_project_study fp, MULTIPROJ.study s, MULTIPROJ.file_item_uuid fu, MULTIPROJ.uuid u
WHERE fi.status = 'Available'
AND   fi.is_latest = 1
AND   fi.file_id = fp.file_id
AND   fp.study_code = s.study_code
AND   fi.file_id = fu.file_id
AND   fu.uuid = u.uuid
AND   fi.data_type_id = d.data_type_id
AND   fi.platform_id = p.platform_id
AND   fp.project_code='ER')
group by alt_id, cid, disease, disease_desc, center, data_type, updated_date
order by alt_id, cid, disease,center, data_type;
"""



import cx_Oracle
import datetime



"""
#QA db info
"""
host = 'ncidb-tcga-q.nci.nih.gov'
port = '1652'
sid = 'TCGAQA'
user = 'readonly'
pwd = '' #password removed for security purpose

total_info = ''
file_info = ''

file_num = ''
file_size = ''
case1_content = ''
case2_content = ''
case3_content = ''
csv_header = "file_name, md5, status, modified_date, center, data_type, data_level, platform_name, Study, Project"

#pat_year = '(?P<year>(?:19|20)\d\d)'
#pat_month = '(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)'
#pat_6d = '(?P<year>\d\d)(?P<delimiter>[- /.])(?P<month>0[1-9]|1[012])\2(?P<day>0[1-9]|[12][0-9]|3[01])'

test_sql="""
select count(file_id) from MULTIPROJ.file_item
"""
#if ( len(sys.argv[:]) != 3  )  and __name__ == '__main__':
#     print 'USAGE anno_load.py file_name server(0 for prod, 1 for dev, 2 for qa)\n'

if  __name__ == '__main__':


    dsn = cx_Oracle.makedsn(host, port, sid)
    con = cx_Oracle.connect(user, pwd, dsn)
    print con
    cur = con.cursor()
    print cur

    #query the total file number and file size
    #print total_sql
    cur.execute(test_sql)
    print 'test done.'
    cur.execute(total_sql)
    for result in cur:
        total_info += str(result).replace('(','').replace(')','')
        print total_info
        file_num = total_info.split(',')[0].strip()
        if total_info.split(',')[-1].strip() != 'None':
            file_size = "%.2f"%(float(total_info.split(',')[-1].strip())/(1024**3))+"GB"
            if file_size[0:4]=='0.00':
                file_size = "%.2f"%(float(total_info.split(',')[-1].strip())/(1024**2))+"MB"
        else:
            file_size = '0 GB'
    print total_info
    print file_num, file_size

    dataByCategories = '"dataByCategories": [\n'
    cur.execute(fcount_by_category_sql)
    for result in cur:
        print result
        dc_name = str(str(result).split(',')[0].replace(')','').replace('(','').replace("'",""))
        dc_value = str(str(result).split(',')[1].replace(')','').replace('(','').replace("'",""))
        dataByCategories += '{"name":"'+dc_name+'", "value":'+dc_value+'},\n'
     
        print dataByCategories
    dataByCategories = dataByCategories[:-2]+'\n'+']'
#json_file_name = 'ER_landing.json'
    print 'final dbc:', dataByCategories
    print 'fcount_sql done.'
    
    
#print case1_sql

    studyData = '"studyData":['+'\n' 
    cur.execute(case1_sql)
    tot_cases = 0
    for result in cur:
        sd_name = str(result).split(',')[0].replace('(','').replace(')','').replace("'","").strip()
        sd_value = str(result).split(',')[1].replace('(','').replace(')','').replace("'","").strip()
        studyData += '{\n'+'"name":"'+sd_name+'",\n'+'"value":'+sd_value+'\n},\n'
        tot_cases += int(sd_value)
        print result
    studyData = studyData[:-2]+'\n]'
    print 'studyData:',studyData 
    print 'case1_sql done.'
    print tot_cases
    
    cur.execute(case_table_sql)
    dataType = '"dataType" : [\n'
    dataType_list = []
    dataType_value_dic = {}
    case_list = []
    update_dic = {}
    for result in cur:
        print str(result)
        result_str = str(result).strip().replace('(','').replace(')','').replace("'","")
        case_id = result_str.split(',')[0].strip()
        alt_id = result_str.split(',')[1].strip()
        if alt_id.startswith('ERRP')==False:
            continue
        disease = result_str.split(',')[2].strip()
        disease_desc = result_str.split(',')[3].strip()
        center = result_str.split(',')[4].strip()
        data_type = result_str.split(',')[5].strip()
        update_date = result_str.split(',')[6].strip()
        value = result_str.split(',')[7].strip()
        if case_id in update_dic:
            if datetime.datetime.strptime(update_dic[case_id],"%m/%d/%Y") < datetime.datetime.strptime(update_date, "%m/%d/%Y"):
                update_dic[case_id] = update_date
        else:
            update_dic[case_id] = update_date
        if case_list.count(case_id+'::'+alt_id+'::'+disease+'::'+disease_desc)==0:
            case_list.append(case_id+'::'+alt_id+'::'+disease+'::'+disease_desc)
        if dataType_list.count(center+'_'+data_type.replace(' ','_'))==0:
            dataType_list.append(center+'_'+data_type.replace(' ','_'))
        dataType_value_dic[case_id+'::'+center+'_'+data_type.replace(' ','_')]=value
        print result
    print dataType_list
    print case_list 
    print dataType_value_dic
    print 'case_table_sql done.'

    dataType = '"tableHeader" : [\n'  
    for dt in dataType_list:
        if not 'linical' in dt:
            dataType += '{"field": "'+dt+'", "displayName": '+'"'+dt.replace('_',' ')+'"},\n'
    dataType += '{"field":"Last_Update","displayName":"Last Update Date"}'  
    dataType = dataType+'\n]\n'
    print 'dataType:', dataType
    

    erData = '"erData":[\n'
    for case in case_list:
       erData += '{\n'+'"participantId":"'+case.split('::')[0]+'",\n'
       erData += '"altParticipantId":"'+case.split('::')[1]+'",\n'
       erData += '"study":"'+case.split('::')[2]+'",\n'
       erData += '"studyDesc":"'+case.split('::')[3]+'",\n'
       for dt in dataType_list:
           
           value = 0
           if dataType_value_dic.has_key(case.split('::')[0]+'::'+dt):
               value = dataType_value_dic[case.split('::')[0]+'::'+dt]
           erData +=  '"'+dt+'":"'+str(value)+'",\n'
       erData += '"Last_Update": "'+update_dic[case.split('::')[0]]+'"'
       erData = erData+'\n},\n'
    erData = erData[:-2]+'\n]'       
    print 'erData:',erData        

    cur.close()
    con.close()
    print 'tot_cases:', tot_cases
    print 'file_num:',file_num
    print 'file_size:',file_size 

    final_out = '{\n' + '"success":true,\n'+'"totalCase":'+str(tot_cases)+',\n'+'"totalDownloadableFiles": '+str(file_num)+',\n'+'"totalFileSize":'+'"'+str(file_size)+'",\n'
    final_out = final_out+ dataType + ',\n' + studyData + ',\n' + dataByCategories + ',\n' + erData + '\n}'
    #import json
    #parsed=json.loads(final_out)     
    fout = open('erData.json','w')
    #fout.write(json.dumps(parsed,sort_keys=False, indent=4))
    fout.write(final_out)
    fout.close()



   
