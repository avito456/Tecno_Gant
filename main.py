import pandas as pd
from datetime import timedelta, datetime
from plotly.figure_factory import create_gantt
from loguru import logger
import glob
import re


logger.add("debug.log", format="{time} {level} {message}",
           level="DEBUG", rotation="100 MB", compression="zip")



class Parser():

    def __init__(self):
        self.data = []


    @logger.catch
    def read_logs(self, dir_name: str):
        files = glob.glob(f'{dir_name}/*/*.log')
        for file in files:
            self.parse_log(file)
        #file = './Logs/QERR/rphost_10248/23020109.log'
        #self.parse_log(file)
        #with open(filename, 'r', encoding="utf-8") as file:
        #    for line in enumerate(file):
        #        if len(line[1]) < 3:
        #            continue
        #return


    @logger.catch
    def parse_log(self, filename: str):

        open_trans = {}
        id_open_trans = {}
        timeouts = {}
        timeouts_v = {}
        deadlocks = {}
        deadlocks_v = {}
        requisites = {}
        limit_duration = 1000

        with open(filename, 'r', encoding="utf-8") as file:
            for line in enumerate(file):
                if len(line[1]) < 3:
                    continue

                #logger.debug(line)
                result = re.search(r'(\d{2})(\d{2})(\d{2})(\d{2}).log$', filename)
                year = result.group(1)
                month = result.group(2)
                day = result.group(3)
                hour = result.group(4)
                date_str = f'20{year}-{month}-{day} {hour}:'

                if not re.search(r'^\d{2}:\d{2}.\d+-', line[1]):
                    continue

                ls = line[1].split(',')
                min = ls[0][:2]
                sec = ls[0][3:5]
                mcrsec = ls[0][6:12]
                duration = ls[0][13:]

                finish = date_str + min + ':' + sec + '.' + mcrsec
                start = datetime.strptime(finish, '%Y-%m-%d %H:%M:%S.%f') - timedelta(microseconds=int(duration))

                requisites.clear()
                requisites['Func'] = ''
                requisites['WaitConnections'] = ''
                requisites['Context'] = ''
                requisites['t:connectID'] = ''
                requisites['Usr'] = ''
                requisites['Regions'] = ''
                already_context = False


                for element in ls:
                    if already_context:
                        requisites['Context'] += element
                        continue
                    # Разбор строки на словарик "requisites"
                    pos = element.find("=")
                    if pos > -1:
                        requisites[element[:pos]] = element[pos + 1:]
                    if element[:pos] == 'Context':
                        already_context = True

                ls_con = requisites['Context'].split('@')
                cnt_ls = len(ls_con)
                if cnt_ls > 1:
                    requisites['Context'] = ls_con[0]+' '+ls_con[cnt_ls-1]

                if ls[1] == 'DBMSSQL':
                    if int(duration) > limit_duration:

                        finish = date_str+min+':'+sec+'.'+mcrsec
                        start = datetime.strptime(finish, '%Y-%m-%d %H:%M:%S.%f') - timedelta(microseconds=int(duration))
                        self.data.append(dict(Task=requisites['Usr']+'_'+requisites['SessionID'], Start=str(start),
                                         Finish=finish, Resource='DBMSSQL', Description="Контекст: " + requisites['Context'] \
                                         + ", Длит.: " + str(int(duration) / 1000000) + "c", Conn=requisites['t:connectID']))

                if ls[1] == 'SDBL':
                    if requisites['Func'] == 'BeginTransaction':
                        open_trans[requisites['Usr']+'_'+requisites['SessionID']] = date_str+min+':'+sec+'.'+mcrsec
                        id_open_trans[requisites['t:connectID']] = requisites['Usr']+'_'+requisites['SessionID']

                    if requisites['Func'] == 'CommitTransaction' or requisites['Func'] == 'RollbackTransaction':
                        if requisites['Usr']+'_'+requisites['SessionID'] in open_trans:
                            if int(duration) > limit_duration:
                                start = open_trans.pop(requisites['Usr']+'_'+requisites['SessionID'])
                                finish = date_str+min+':'+sec+'.'+mcrsec

                                if requisites['Usr']+'_'+requisites['SessionID']+'_'+requisites['t:connectID'] in deadlocks:
                                    self.data.append(
                                        dict(Task=requisites['Usr'] + '_' + requisites['SessionID'], Start=str(start),
                                             Finish=finish, Resource='SDBLd', Description=requisites['Context'],
                                             Conn=requisites['t:connectID']))
                                    deadlocks.pop(requisites['Usr']+'_'+requisites['SessionID']+'_'+requisites['t:connectID'])

                                elif requisites['t:connectID'] in deadlocks_v:
                                    self.data.append(
                                        dict(Task=requisites['Usr'] + '_' + requisites['SessionID'], Start=str(start),
                                             Finish=finish, Resource='SDBLd', Description=requisites['Context'],
                                             Conn=requisites['t:connectID']))
                                    deadlocks_v.pop(requisites['t:connectID'])

                                elif requisites['t:connectID'] in id_open_trans and id_open_trans[
                                    requisites['t:connectID']] == "WAIT":
                                    self.data.append(dict(Task=requisites['Usr'] + '_' + requisites['SessionID'],
                                                     Start=str(start), Finish=finish, Resource='SDBLw',
                                                     Description=requisites['Context'], Conn=requisites['t:connectID']))

                                elif requisites['t:connectID'] in timeouts_v:
                                    self.data.append(
                                        dict(Task=requisites['Usr'] + '_' + requisites['SessionID'], Start=str(start),
                                             Finish=finish, Resource='SDBLt', Description=requisites['Context'],
                                             Conn=requisites['t:connectID']))
                                    timeouts_v.pop(requisites['t:connectID'])

                                else:
                                    self.data.append(
                                        dict(Task=requisites['Usr'] + '_' + requisites['SessionID'], Start=str(start),
                                             Finish=finish, Resource='SDBL', Description=requisites['Context'],
                                             Conn=requisites['t:connectID']))

                                if requisites['t:connectID'] in id_open_trans:
                                    id_open_trans.pop(requisites['t:connectID'])

                if ls[1] == 'TLOCK':
                    if requisites['WaitConnections'] != '':
                        finish = date_str + min + ':' + sec + '.' + mcrsec
                        start = datetime.strptime(finish, '%Y-%m-%d %H:%M:%S.%f') - timedelta(microseconds=int(duration))
                        key = requisites['Usr'] + '_' + requisites['SessionID'] + '_' + requisites['t:connectID'] + '_' + \
                              requisites['WaitConnections'] + '_' + date_str + min + ':' + sec

                        if key in timeouts:
                            self.data.append(dict(Task=requisites['Usr'] + '_' + requisites['SessionID'], Start=str(start),
                                    Finish=finish, Resource='TTIMEOUT', Description="Ресурс: " + requisites['Regions'] + \
                                    ", " + timeouts[key], Conn=requisites['t:connectID']))
                        else:
                            val = ''
                            for dic in range(len(self.data) - 1, -1, -1):
                                if self.data[dic]["Conn"] == requisites['WaitConnections'] and self.data[dic]["Resource"] == 'SDBL':
                                    self.data[dic]["Resource"] = 'SDBLw'
                                    val = self.data[dic]["Task"]
                                    break
                            self.data.append(dict(Task=requisites['Usr'] + '_' + requisites['SessionID'], Start=str(start),
                                             Finish=finish, Resource='TLOCK', Description="Виновник: " + val + "  Ресурс: " \
                                                                                          + requisites['Regions'] +
                                                                                          requisites['WaitConnections'],
                                             Conn=requisites['t:connectID']))

                if ls[1] == 'TTIMEOUT':
                    val = ''
                    if requisites['WaitConnections'] in id_open_trans:
                        val = id_open_trans[requisites['WaitConnections']]
                    timeouts[requisites['Usr']+'_'+requisites['SessionID']+'_'+requisites['t:connectID']+'_'+ \
                        requisites['WaitConnections']+'_'+date_str+min+':'+sec] = "Виновник: " + val + " Контекст: " \
                                                                                          + requisites['Context']
                    timeouts_v[requisites['WaitConnections']] = val

                if ls[1] == 'TDEADLOCK':
                    val = ''
                    l1 = requisites['DeadlockConnectionIntersections'].split()

                    if l1[1] in id_open_trans:
                        val = id_open_trans[l1[1]]

                    deadlocks[requisites['Usr']+'_'+requisites['SessionID']+'_'+l1[0][1:]] = "Виновник: " \
                        + val + "Контекст: " + requisites['Context']

                    deadlocks_v[l1[1]] = val


    @logger.catch
    def view_gant(self):
        df = pd.DataFrame(self.data)
        colors = {'SDBL': 'rgb(0, 128, 1)',
                  'SDBLw': 'rgb(255, 255, 0)',
                  'SDBLt': 'rgb(255, 91, 0)',
                  'SDBLd': 'rgb(227, 38, 54)',
                  'DBMSSQL': 'rgb(0, 0, 255)',
                  'TLOCK': 'rgb(255, 255, 0)',
                  'TTIMEOUT': 'rgb(255, 91, 0)',
                  'TDEADLOCK': 'rgb(255, 0, 0)'
                  }

        fig = create_gantt(df, group_tasks=True, index_col='Resource', colors=colors, bar_width=0.1, showgrid_x=True,
                           showgrid_y=True)
        fig.show()



if __name__ == '__main__':
    parser = Parser()
    parser.read_logs('./TG')
    parser.view_gant()
