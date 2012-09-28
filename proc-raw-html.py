#!/usr/bin/python

''' yammer yammer

turn html page full of tables into maps of 
metadata -> data point

this can then, in theory, be turned into the
table format easily.
'''

import sys, re, os
import BeautifulSoup

#  converts a beautiful soup table object into
#  a 2d array.  swiped from here:
#  http://stackoverflow.com/questions/2870667/how-to-convert-an-html-table-to-an-array-in-python
#  
#  also prepends 'xxCOLORxx' to rows with a color.  this is used later
#  to find when data rows begin
def makelist(table):
    result = []
    allrows = table.findAll('tr')
    for row in allrows:
        hascolor=False
        if row.has_key('bgcolor'):
            hascolor=True
        result.append([])
        allcols = row.findAll('td')
        for col in allcols:
            thetext=""
            if hascolor:
                thetext="xxCOLORxx"
            thestrings = [unicode(s) for s in col.findAll(text=True)]
            thetext += ''.join(thestrings)
            repeat=1
            if col.has_key('colspan'):
                repeat=int(col['colspan'])
            count = 0
            while count < repeat:
                result[-1].append(thetext)
                count += 1
    return result

#  from the same as makelist
def get_tables(htmldoc):
    soup = BeautifulSoup.BeautifulSoup(htmldoc)
    return soup.findAll('table')

#  appends sourceheader to headerrow at each cell
#  returns headderow by value
def build_header(headerrow, sourceheader):
    carry_cell = ""
    idx = 0
    if sourceheader[0].startswith('xxCOLORxx'):
        return False
    while idx < len(sourceheader):
        if re.match(".*\w.*",sourceheader[idx]):
            carry_cell = sourceheader[idx]

        headerrow[idx] += " | " + carry_cell
        idx += 1

    return True

#  process a metadata/data row
#  has two return values:
#    1. explicit return of current extraneous list
#    2. procedtable return by value, it gets modified
def process_row(headerrow, procedtable, datarow, extraneous):
    is_extraneous=True
    is_clearing_row=False
    datarow[0] = re.sub('xxCOLORxx', '', datarow[0])
    if datarow[0] == "":
        is_clearing_row=True

    #  only add a header cell once per key
    is_new=True
    for excel in extraneous:
        if excel.startswith(headerrow[0]):
            is_new=False

    if is_new:
        extraneous.append(headerrow[0] + " " + datarow[0])
    else:
        extraneous.append(datarow[0])

    didx = 1
    while didx < len(datarow):
        datarow[didx] = re.sub('xxCOLORxx', '', datarow[didx])
        if re.match(".*\w.*",datarow[didx]):
            is_extraneous=False
            is_clearing_row=False
            if re.match('[a-zA-Z]',datarow[didx]):    #  catches 'located on ?? section of other report'
                extraneous[-1] += headerrow[didx] + " " + datarow[didx]
            else:
                pctbl_key = headerrow[didx]
                for excel in extraneous:
                    pctbl_key += " | " + excel

                #  remove new lines, fucking with my chi
                pctbl_key = re.sub("\n", " ", pctbl_key)

                if pctbl_key not in procedtable:
                    procedtable[pctbl_key] = datarow[didx] # re.sub("\n", " ", datarow[didx])
                else:
                    procedtable[pctbl_key] += datarow[didx] # re.sub("\n", " ", datarow[didx])
        didx += 1
            
    if not is_extraneous:
        extraneous.pop()

    if is_clearing_row:
        extraneous=list()

    return extraneous

#  statemachine
def process_table(tbl_list, all_elems):
    headerrow = [""]*len(tbl_list[0])
    extraneous = list()
    header_flip = True
    for row in tbl_list:
        if header_flip:
            header_flip = build_header(headerrow, row)
            #  this algorithm grabs one row to many to flip from header to data row
            #  it sees the data row in the build header function so we need to then
            #  process the first data row as such
            if not header_flip:
                extraneous = process_row(headerrow, all_elems, row, extraneous)

        else:
            extraneous = process_row(headerrow, all_elems, row, extraneous)

    return all_elems 

#  MAIN CODE
report_fd = open("dat-files/STAR_GAS_PARTNERS_LP_10q_2011q1.dat")
report_data = report_fd.read()

all_tables = get_tables(report_data)
all_elems = dict()
for tbl in all_tables:
    tbl_list = makelist(tbl)
    tbl_list_clean = [[re.sub('&nbsp;',' ',str(x)) for x in y] for y in tbl_list]
    process_table(tbl_list_clean, all_elems)

output_fd = open("dict-dump.txt", "w")
for k,v in all_elems.iteritems():
    output_fd.write("Key: %s \t Value: %s\n" % (k, v))

print "DONE"
