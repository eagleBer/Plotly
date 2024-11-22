# -*- coding: utf-8 -*-
"""
Created on Wed Aug 28 13:15:32 2019

@author: jonroe
"""

import pyodbc    # for the database query 
import pandas    # for dataframes
import re
import math
#import matplotlib.pyplot
from sqlalchemy import create_engine
#from sqlalchemy.orm import sessionmaker

#------------------------------------------------------------------------------
#------------------------------------------------------------------------------

# List of IDs for messtyp
MESSTYP_ID = {"BFY-Puls"      : "1", 
              "BFW-Messung"   : "2", 
              "Kennlinie"     : "3", 
              "Spektrum"      : "4", 
              "BurnIn"        : "5", 
              "KennlinienMap" : "6", 
              "SpektrumMap"   : "7", 
              "Per"           : "8",               
              "Strahlprofil"  : "9"}

# List of Messgrund
MESSGRUND_ID = {"Messung1"             : "1",
                "Messung2"             : "2",
                "MessungVorPackaging"  : "7",
                "Qualifikation"        : "3",
                "GoldenDevice"         : "4",
                "Nachprüfung"          : "6",
                "Wafer_Qualifikation"  : "48"}

# Names of the tables that contain the data for the specific types 
TABLE = {"Kennlinie"     : "M_Kennlinie", 
         "Spektrum"      : "M_Spektrum", 
         "BurnIn"        : "M_BurnIn", 
         "KennlinienMap" : "M_Kennlinie", 
         "SpektrumMap"   : "M_Spektrum", 
         "Per"           : "M_Per", 
         "Strahlprofil"  : "M_Strahlprofil"}

# Names of the column that contains the filenames in the table above 
FILENAME = {"Kennlinie"     : "klMessdatei", 
            "Spektrum"      : "spMessdatei", 
            "BurnIn"        : "biMessdatei", 
            "KennlinienMap" : "klMessdatei", 
            "SpektrumMap"   : "spMessdatei", 
            "Per"           : "peDateiName", 
            "Strahlprofil"  : "stpMessdatei"}

# Shortages for each type as used in the database 
SHORTAGE = {"Kennlinie"     : "kl", 
            "Spektrum"      : "sp", 
            "BurnIn"        : "bi", 
            "KennlinienMap" : "kl", 
            "SpektrumMap"   : "sp", 
            "Per"           : "pe", 
            "Strahlprofil"  : "stp"}

# auto filter for each messtyp
auto_filters = {

# filters for Kennlinie measurements
"Kennlinie": list((
#        ("Messgrund",               ""),
#        ("Testfeld",                ""),
#        ("Riegelnummer",            ""),
#        ("Chipnummer",              ""),
#        ("Coating",                 ""),
#        ("ID",                      ""),
#        ("Messplatz",               ""),
#        ("MpAktKonf_ID",            ""),
#        ("Messgrund_ID",            ""),
#        ("Diode_ID",                ""),
#        ("Pfad_ID",                 ""),
#        ("Messdatei",               ""),
#        ("Temperatur",              ""),
#        ("Ith",                     "!0"),
#        ("Slope",                   "!0"),
#        ("Rs",                      "!0"),
#        ("IatPopt",                 ["!0", "!9999999"]),
#        ("UatPopt",                 ["!-1", "!9999999"]),
#        ("Popt",                    ""),
#        ("Datum",                   ""),
#        ("ImonAtPopt",              "!0"),
#        ("ImonSlope",               "!0"),
#        ("IthAusM",                 "!0"),
#        ("SlopeAusM",               "!0"),
#        ("MdiffMax",                ["!-1", "!0"]),
#        ("PkinkFree",               "0"),
#        ("IkinkFree",               ""),
#        ("ImonSlopeRideal",         ""),
#        ("ImonSlopeRavg",           ""),
#        ("ImonRdelta",              ""),
#        ("MdrAtPopt",               ""),
#        ("UePeakPower",             ""),
#        ("IdPeakPower",             ""),
#        ("PeakPower",               ""),
#        ("FehlerCode",              ""),
#        ("Irated",                  ""),
#        ("SlopeAt",                 ""),
#        ("PlossAt",                 ""),
#        ("KonversionseffizienzAt",  ""),
#        ("Meta_ID",                 ""),
#        ("Sachnummer",              ""),
#        ("Header",                  ""),
)),

# filters for Spektrum measurements
"Spektrum": list((
#        ("Messgrund",               ""),
#        ("Testfeld",                ""),
#        ("Riegelnummer",            ""),
#        ("Chipnummer",              ""),
#        ("Coating",                 ""),
#        ("ID",                      ""),
#        ("Messplatz",               ""),
#        ("MpAktKonf_ID",            ""),
#        ("Messgrund_ID",            ""),
#        ("Diode_ID",                ""),
#        ("EmiRange_ID",             ""),
#        ("Pfad_ID",                 ""),
#        ("Messdatei",               ""),
#        ("Datum",                   ""),
#        ("Temperatur",              ""),
#        ("Span",                    ""),
#        ("Resolution",              ""),
#        ("Wavelength",              ""),
#        ("LambdaGaussfit",          ""),
#        ("Fwhm",                    ""),
#        ("FwhmGewichtet",           ""),
#        ("LambdaMin",               ""),
#        ("LambdaMax",               ""),
#        ("I",                       ""),
#        ("Popt",                    ""),
#        ("Smsr",                    ""),
#        ("SmsrNp",                  ""),
#        ("Rausch",                  ""),
#        ("PoptMaxSpektrum",         ""),
#        ("Irated",                  ""),
#        ("FehlerCode",              ""),
#        ("Meta_ID",                 ""),
#        ("Sachnummer",              ""),
#        ("Header",                  ""),
)),

# filters for BurnIn measurements
"BurnIn": list((
#        ("Messgrund",               ""),
#        ("Testfeld",                ""),
#        ("Riegelnummer",            ""),
#        ("Chipnummer",              ""),
#        ("Coating",                 ""),
#        ("ID",                      ""),
#        ("Messplatz",               ""),
#        ("MpAktKonf_ID",            ""),
#        ("Diode_ID",                ""),
#        ("Pfad_ID",                 ""),
#        ("Messdatei",               ""),
#        ("Datum",                   ""),
#        ("Degradation",             ""),
#        ("Messtemperatur",          ""),
#        ("Popt",                    ""),
#        ("I",                       ""),
#        ("Messgrund_ID",            ""),
#        ("Meta_ID",                 ""),
#        ("Sachnummer",              ""),
#        ("Header",                  ""),
)),

# filters for Per measurements
"Per": list((
#        ("Messgrund",               ""),
#        ("Testfeld",                ""),
#        ("Riegelnummer",            ""),
#        ("Chipnummer",              ""),
#        ("Coating",                 ""),
#        ("ID",                      ""),
#        ("Messplatz",               ""),
#        ("Per",                     ""),
#        ("Temperature",             ""),
#        ("Current",                 ""),
#        ("Meta_ID",                 ""),
#        ("Pfad_ID",                 ""),
#        ("DateiName",               ""),
#        ("Sachnummer",              ""),
#        ("Header",                  ""),
)),

# filters for Strahlprofil measurements
"Strahlprofil": list((
#        ("Messgrund",               ""),
#        ("Testfeld",                ""),
#        ("Riegelnummer",            ""),
#        ("Chipnummer",              ""),
#        ("Coating",                 ""),
#        ("ID",                      ""),
#        ("Messplatz",               ""),
#        ("Messgrund_ID",            ""),
#        ("Diode_ID",                ""),
#        ("Pfad_ID",                 ""),
#        ("Messdatei",               ""),
#        ("Camera",                  ""),
#        ("Typ",                     ""),
#        ("Gain",                    ""),
#        ("Distance",                ""),
#        ("CenterX_mm",              ""),
#        ("CenterY_mm",              ""),
#        ("Dx_mm",                   ""),
#        ("Dy_mm",                   ""),
#        ("DHor_mm",                 ""),
#        ("DVer_mm",                 ""),
#        ("CenterX_grad",            ""),
#        ("CenterY_grad",            ""),
#        ("Dx_grad",                 ""),
#        ("Dy_grad",                 ""),
#        ("Ratio",                   ""),
#        ("Alpha",                   ""),
#        ("Datum",                   ""),
#        ("PcA_Diameter",            ""),
#        ("PcB_Diameter",            ""),
#        ("Powerlevel_A",            ""),
#        ("Powerlevel_B",            ""),
#        ("PowerlevelRatio_AB",      ""),
#        ("Meta_ID",                 ""),
#        ("Sachnummer",              ""),
#        ("Header",                  ""),
))}

# cleartext for each column
cleartext = {"mgMessgrund":                 "",
             "snSachnummer":                "",
             "riTestfeld":                  "",
             "riRiegelnummer":              "",
             "diChipNr":                    "",
             "chCoatNr":                    "",
             "mpName":                      "",
             "hnHeaderNr":                  "",
             "diAuftrag_ID":                "",
             "meDiode_ID":                  "",
             "klID":                        "",
             "klDiode_ID":                  "",
             "klMpAktKonf_ID":              "",
             "klMessgrund_ID":              "",
             "klPfad_ID":                   "",
             "klMessdatei":                 "",
             "klTemperatur":                u"in °C",
             "klIth":                       "in mA",
             "klSlope":                     "in W/A",
             "klRs":                        "in Ohm",
             "klIatPopt":                   "in mA",
             "klUatPopt":                   "in V",
             "klPopt":                      "in mW",
             "klDatum":                     "Messdatum",
             "klImonAtPopt":                "in mA",
             "klImonSlope":                 "in W/A",
             "klIthAusM":                   "in mA",
             "klSlopeAusM":                 "in W/A",
             "klMdiffMax":                  "in mA",
             "klPkinkFree":                 "in mW",
             "klIkinkFree":                 "in mA",
             "klImonSlopeRideal":           "",
             "klImonSlopeRavg":             "",
             "klImonRdelta":                "",
             "klMdrAtPopt":                 "in mW",
             "klUePeakPower":               "in V",
             "klIdPeakPower":               "in mA",
             "klPeakPower":                 "in W",
             "klFehlerCode":                "",
             "klIrated":                    "",
             "klSlopeAt":                   "",
             "klPlossAt":                   "",
             "klKonversionseffizienzAt":    "",
             "klMeta_ID":                   "",
             "spID":                        "",
             "spMpAktKonf_ID":              "",
             "spMessgrund_ID":              "",
             "spEmiRange_ID":               "",
             "spPfad_ID":                   "",
             "spMessdatei":                 "",
             "spDatum":                     "",
             "spTemperatur":                u"in °C",
             "spSpan":                      "",
             "spResolution":                "",
             "spWavelength":                "in nm",
             "spLambdaGaussfit":            "in nm",
             "spFwhm":                      "",
             "spFwhmGewichtet":             "",
             "spLambdaMin":                 "in nm",
             "spLambdaMax":                 "in nm",
             "spI":                         "in mA",
             "spPopt":                      "in mW",
             "spSmsr":                      "in dB",
             "spSmsrNp":                    "in dB",
             "spRausch":                    "in dBm",
             "spPoptMaxSpektrum":           "",
             "spIrated":                    "",
             "spFehlerCode":                "",
             "spMeta_ID":                   "",
             "biID":                        "",
             "biMpAktKonf_ID":              "",
             "biPfad_ID":                   "",
             "biMessdatei":                 "",
             "biDatum":                     "",
             "biDegradation":               "",
             "biMesstemperatur":            u"in °C",
             "biPopt":                      "in mW",
             "biI":                         "in mA",
             "biMessgrund_ID":              "",
             "biMeta_ID":                   "",
             "peID":                        "",
             "pePer":                       "",
             "peTemperature":               u"in °C",
             "peCurrent":                   "in mA",
             "peMeta_ID":                   "",
             "pePfad_ID":                   "",
             "peDateiName":                 "",
             "stpID":                       "",
             "stpMessgrund_ID":             "",
             "stpPfad_ID":                  "",
             "stpMessdatei":                "",
             "stpCamera":                   "",
             "stpTyp":                      "",
             "stpGain":                     "",
             "stpDistance":                 "",
             "stpCenterX_mm":               "",
             "stpCenterY_mm":               "",
             "stpDx_mm":                    "",
             "stpDy_mm":                    "",
             "stpDHor_mm":                  "",
             "stpDVer_mm":                  "",
             "stpCenterX_grad":             "",
             "stpCenterY_grad":             "",
             "stpDx_grad":                  "",
             "stpDy_grad":                  "",
             "stpRatio":                    "",
             "stpAlpha":                    "",
             "stpDatum":                    "",
             "stpPcA_Diameter":             "",
             "stpPcB_Diameter":             "",
             "stpPowerlevel_A":             "",
             "stpPowerlevel_B":             "",
             "stpPowerlevelRatio_AB":       "",
             "stpMeta_ID":                  "",
             "chChargenNr":                 "",
             "chProzessNr":                 "",
             "klProzessNr":                 "",
             "spProzessNr":                 ""} # added matreg 20240819 Charge

#------------------------------------------------------------------------------
#------------------------------------------------------------------------------

def where_str_from_filters(filters, messtyp):
    """
    This funtion generates the "where-string" for a database query, where the 
    chosen filters are applied. 
    """
    where_str = ""
    
    # for every given filter
    for (column, value) in filters:
        
        positive_str = ""
        negative_str = ""
        
        # get the name of the column in the database
        column_name = SHORTAGE[messtyp] + column
        if column == "Messgrund":
            column_name = "mgMessgrund"
        elif column == "Sachnummer":
            column_name = "snSachnummer"
        elif column == "Testfeld":
            column_name = "riTestfeld"
        elif column == "Riegelnummer":
            column_name = "riRiegelnummer"
        elif column == "Chipnummer":
            column_name = "diChipNr"
        elif column == "Coating":
            column_name = "chCoatNr"
        elif column == "Messplatz":
            column_name = "mpName"
        elif column == "Header":
            column_name = "hnHeaderNr"
        elif column == "Los":
            column_name = "diAuftrag_ID"
        elif column == "M_Messungen" :#Dioden-ID":
            column_name = "meDiode_ID"
        elif column == "Charge":        # added matreg 20240819 Charge
            column_name = "chProzessNr", #chChargenNr
        elif column == "ProzessNr":
            column_name ="chProzessNr"
        elif column == "FehlerCode":
            column_name = "fcFehlerText"
      
       
        # if only one value is given
        if not type(value) == type([]):
            value = [value]
        
        # all values for a specific column are written in brackets
        for v in value:
            
            # if a string is given
            if type(v) == type(""):
                
                # if a value should be excluded
                if v.startswith("!"):
                    negative_str += "%s != '%s' AND " % (column_name, v[1:])
                """
                # if a range is given
                elif ";" in v:
                    left = v[:v.index(";")]
                    right = v[v.index(";") + 1:]
                    positive_str += "(%s <= '%s' AND %s >= '%s') OR " % (column_name, right, column_name, left)
                
                # otherwise
                else:
                    positive_str += "%s = '%s' OR " % (column_name, v)
"""            
            # if a number is given
            else:
                positive_str += "%s = '%s' OR " % (column_name, v)
        
        if positive_str != "" and negative_str != "":
            where_str += " AND %s AND (%s)" % (negative_str[:-5], positive_str[:-4])
        elif positive_str == "":
            where_str += " AND %s" % negative_str[:-5]
        elif negative_str == "":
            where_str += " AND (%s)" % positive_str[:-4]
    
    # return the result
    return where_str

#------------------------------------------------------------------------------
#------------------------------------------------------------------------------

def table_sn(sachnummer, messtyp, columns, filters = [], refilters = [], clear_text = True, 
             sort_out_duplicates = False, auto_filtering = False):
    
    """
    This function searches the database for measurements of the given sachnummer 
    and messtyp(bzw.Messgrund) and gives back the given columns. 
    
    example:
        table_sn("EYP-DFB-1064-00040-1500-BFY02-0001", "Spektrum", ["Temperatur", "I", "Wavelength"])
    """
    
    # at least one column has to be given
    assert len(columns) > 0
    
    # setting up the string for the query
    select_str = "SELECT %sPfad_ID, %s, meDiode_ID" % (SHORTAGE[messtyp], FILENAME[messtyp])
    for column in columns:
        if column == "Messgrund":
            select_str += ", mgMessgrund"
        elif column == "Sachnummer":
            select_str += ", snSachnummer"
        elif column == "Testfeld":
            select_str += ", riTestfeld"
        elif column == "Riegelnummer":
            select_str += ", riRiegelnummer"
        elif column == "Chipnummer":
            select_str += ", diChipNr"
        elif column == "Coating":
            select_str += ", chCoatNr"
        elif column == "Messplatz":
            select_str += ", mpName"
        elif column == "Header":
            select_str += ", hnHeaderNr"
        elif column == "Los":
            select_str += ", diAuftrag_ID"
        elif column == "Dioden-ID":
            select_str += ", meDiode_ID"
        elif column == "Charge":            # added matreg 20240819 Charge
           select_str += ", chChargenNr"   #
        elif column == "ProzessNr":
            select_str += ", chProzessNr"
        elif column =="SlopeMessung2":
            select_str +=", spI",
        elif column == "FehlerCode":
            select_str += ", fcFehlerText"
              
        else:
            select_str += ", %s%s" % (SHORTAGE[messtyp], column)
    select_str += " \n"
    from_str  =  "FROM %s\n" % TABLE[messtyp]
    join_str  =  "INNER JOIN M_Messungen ON %sMeta_ID = meID \n" % SHORTAGE[messtyp]
    join_str  += "INNER JOIN M_Messgrund ON meMessgrund_ID = mgID \n"
    join_str +=  "INNER JOIN P_HeaderNr ON meDiode_ID = hnDiode_ID \n"
    join_str +=  "INNER JOIN P_Diode ON meDiode_ID = diID \n"
    join_str +=  "INNER JOIN P_Riegelsachnummer ON diRiegelSachnummer_ID = risnID \n"
    join_str +=  "INNER JOIN P_Riegel ON risnRiegel_ID = riID \n"
    join_str +=  "INNER JOIN P_Charge ON riCharge_ID = chID \n"
    join_str +=  "INNER JOIN M_MpHwSwKonf ON meMpHwSwKonf_Id = mhsID \n"
    join_str +=  "INNER JOIN M_Messplatz ON mhsMessplatz_ID = mpID \n"
    join_str +=  "INNER JOIN P_Auftrag ON diAuftrag_ID = auID \n"
    join_str +=  "INNER JOIN DB_Sachnummer ON auSachnummer_ID = snID \n"
    join_str +=  "INNER jOIN P_FehlerCode ON meMessgrund_ID = fcID \n"
    
    
    where_str =  "WHERE meMesstyp_ID = " + MESSTYP_ID[messtyp]
    
    # if only one is given
    if type(sachnummer) == type(""):
        where_str += " AND snSachnummer = '%s' " % sachnummer
    
    # otherwise
    else:
        where_str += " AND (snSachnummer = '%s'" % sachnummer[0]
        for sn in sachnummer[1:]:
            where_str += " OR snSachnummer = '%s'" % sn
        where_str += ")"
    
    # if auto filtering is chosen
    if auto_filtering:
        for (column, f) in auto_filters[messtyp]:
            if not column in [f[0] for f in filters]:
                filters.append((column, f))
    
    # if filters are given or generated by auto filtering
    if not filters == []:
        where_str += where_str_from_filters(filters, messtyp)
    
    # adding basic strings together
    sql_str = select_str  + from_str + join_str + where_str
    
    #print(sql_str)
    
    # setting up connection
    conn = pyodbc.connect('DRIVER={SQL Server};'
                          'SERVER=sql01.eyp.local;'
                          'Trusted_Connection=yes;')
    cursor = conn.cursor()
    
    # executing query
    cursor.execute(sql_str)
    
    # saving results
    data = cursor.fetchall()
    
    # set up data for dataframe
    new_data = []
    
    # if duplicates should be sorted out
    if sort_out_duplicates:
        
        # If measurements have been conducted multiple times, only the last one 
        # will be given back. 
        # Since the measurements will be saved in the same path with almost the 
        # same name, a dictionary saving all measurements of a path is set up here.
        files = {}
        for entry in data:
            
            # only note .dat files
            if entry[1].endswith('.dat') or (entry[1].endswith('.tif') and messtyp == "Strahlprofil") or (entry[1].endswith('.txt') and messtyp == "Per"):
                
                # the path-ID is the first entry in the data list
                path = entry[0]
                
                # if no files in the same path have been entered yet
                if not path in files.keys():
                    # the path file is already the index so it won't be saved into the dictionary
                    files[path] = [entry[1:]]
                
                # in case there have been files from the same path entered into the dictionary
                else:
                    # control variable to check whether current file has been entered yet
                    added = False
                    
                    # compare every file that has been entered into the same path
                    for i in range(len(files[path])):
                        
                        # get the number at the end of the filename and the prefix of both files
                        a, pre1 = number_of_file(files[path][i][0])
                        b, pre2 = number_of_file(entry[1])
                        
                        # if they don't have the same prefix, they are different measurements
                        if pre1 != pre2:
                            continue
                        
                        # if the already entered file is newer, don't enter the current file
                        elif a > b:
                            break
                        
                        # if the current file is newer than the entered file
                        elif a < b:
                            # remove the older file
                            files[path].remove(files[path][i])
                            
                            # add current file if not already entered
                            if not added:
                                files[path].append(entry[1:])
                                added = True
                                
                        # if both measurements are the same file
                        elif a == b:
                            files[path].append(entry[1:])
                            break
        
        # enter the files for every path
        for key in files.keys():
            for entry in files[key]:
                new_data.append(entry[1:])
    
    # if duplicates should be included
    else:
        for d in data:
            new_data.append(d[2:])
    
    # if units exist, add them
    if clear_text:
        for i in range(len(columns)):
            column = columns[i]
            # get the name of the column in the database
            column_name = SHORTAGE[messtyp] + column

            if column == "Messgrund":
                column_name = "mgMessgrund"
            elif column == "Sachnummer":
                column_name = "snSachnummer"
            elif column == "Testfeld":
                column_name = "riTestfeld"
            elif column == "Riegelnummer":
                column_name = "riRiegelnummer"
            elif column == "Chipnummer":
                column_name = "diChipNr"
            elif column == "Coating":
                column_name = "chCoatNr"
            elif column == "Messplatz":
                column_name = "mpName"
            elif column == "Header":
                column_name = "hnHeaderNr"
            elif column == "Los":
                column_name = "diAuftrag_ID"
            elif column == "Dioden-ID":     # added matreg 20240820 Charge
                column_name = "meDiode_ID"  #
            elif column == "Charge":        # added matreg 20240819 Charge
                column_name = "chProzessNr" # 
            #elif column == "ProzessNr":
             #   column_name == "chProzessNr"
            if not cleartext[column_name] == "":
                # 'column + "." + ' added 20240809 matreg
                columns[i] = column + "." + cleartext[column_name] 
    
    # create dataframe
    df = create_df(new_data, columns)
    
    # give back data
    return filter_table_re(df, refilters)

#------------------------------------------------------------------------------
#------------------------------------------------------------------------------

def table_coating(coating, messtyp, columns, filters = [], refilters = [], clear_text = True, 
                  sort_out_duplicates = False, auto_filtering = False):
    """
    This function searches the database for measurements of the given coating 
    and messtyp and gives back the given columns. 
    
    example:
        table_coating("5308", "Spektrum", ["Messgrund", "Temperatur"])
    """
    
    # at least one column has to be given
    assert len(columns) > 0
    
    # setting up the string for the query
    select_str = "SELECT %sPfad_ID, %s, meDiode_ID" % (SHORTAGE[messtyp], FILENAME[messtyp])
    for column in columns:
        if column == "Messgrund":
            select_str += ", mgMessgrund"
        elif column == "Sachnummer":
            select_str += ", snSachnummer"
        elif column == "Testfeld":
            select_str += ", riTestfeld"
        elif column == "Riegelnummer":
            select_str += ", riRiegelnummer"
        elif column == "Chipnummer":
            select_str += ", diChipNr"
        elif column == "Coating":
            select_str += ", chCoatNr"
        elif column == "Messplatz":
            select_str += ", mpName"
        elif column == "Header":
            select_str += ", hnHeaderNr"
        elif column == "Los":
            select_str += ", diAuftrag_ID"
        elif column == "Dioden-ID":
            select_str += ", meDiode_ID"
        elif column == "Charge":
            select_str += ", chChargenNr"#", chProzessNr " 
        elif column =="ProzessNr":
            select_str += ", chProzessNr"
       
        else:
            select_str += ", %s%s" % (SHORTAGE[messtyp], column)
    select_str += " \n"
    from_str  =  "FROM %s\n" % TABLE[messtyp]
    join_str  =  "INNER JOIN M_Messungen ON %sMeta_ID = meID \n" % SHORTAGE[messtyp]
    join_str  += "INNER JOIN M_Messgrund ON meMessgrund_ID = mgID \n"
    join_str +=  "INNER JOIN P_HeaderNr ON meDiode_ID = hnDiode_ID \n"
    join_str +=  "INNER JOIN P_Diode ON meDiode_ID = diID \n"
    join_str +=  "INNER JOIN P_Riegelsachnummer ON diRiegelSachnummer_ID = risnID \n"
    join_str +=  "INNER JOIN P_Riegel ON risnRiegel_ID = riID \n"
    join_str +=  "INNER JOIN P_Charge ON riCharge_ID = chID \n"
    join_str +=  "INNER JOIN M_MpHwSwKonf ON meMpHwSwKonf_Id = mhsID \n"
    join_str +=  "INNER JOIN M_Messplatz ON mhsMessplatz_ID = mpID \n"
    join_str +=  "INNER JOIN P_Auftrag ON diAuftrag_ID = auID \n"
    join_str +=  "INNER JOIN DB_Sachnummer ON auSachnummer_ID = snID \n"
    
    where_str =  "WHERE meMesstyp_ID = " + MESSTYP_ID[messtyp]
    
    # if only one is given
    if type(coating) == type(""):
        where_str += " AND chCoatNr = '%s' " % coating
    
    # otherwise
    else:
        where_str += " AND (chCoatNr = '%s'" % coating[0]
        for c in coating[1:]:
            where_str += " OR chCoatNr = '%s'" % c
        where_str += ")"
    
    # if auto filtering is chosen
    if auto_filtering:
        for (column, f) in auto_filters[messtyp]:
            if not column in [f[0] for f in filters]:
                filters.append((column, f))
    
    # if filters are given or generated by auto filtering
    if not filters == []:
        where_str += where_str_from_filters(filters, messtyp)
    
    # adding basic strings together
    sql_str = select_str + from_str + join_str + where_str
    
    # setting up connection
    conn = pyodbc.connect('DRIVER={SQL Server};'
                          'SERVER=sql01.eyp.local;'
                          'Trusted_Connection=yes;')
    cursor = conn.cursor()
    
    # executing query
    cursor.execute(sql_str)
    
    # saving results
    data = cursor.fetchall()
    
    # set up data for dataframe
    new_data = []
    
    # if duplicates should be sorted out
    if sort_out_duplicates:
        
        # If measurements have been conducted multiple times, only the last one 
        # will be given back. 
        # Since the measurements will be saved in the same path with almost the 
        # same name, a dictionary saving all measurements of a path is set up here.
        files = {}
        for entry in data:
            
            # only note .dat files
            if entry[1].endswith('.dat') or (entry[1].endswith('.tif') and messtyp == "Strahlprofil") or (entry[1].endswith('.txt') and messtyp == "Per"):
                
                # the path-ID is the first entry in the data list
                path = entry[0]
                
                # if no files in the same path have been entered yet
                if not path in files.keys():
                    # the path file is already the index so it won't be saved into the dictionary
                    files[path] = [entry[1:]]
                
                # in case there have been files from the same path entered into the dictionary
                else:
                    # control variable to check whether current file has been entered yet
                    added = False
                    
                    # compare every file that has been entered into the same path
                    for i in range(len(files[path])):
                        
                        # get the number at the end of the filename and the prefix of both files
                        a, pre1 = number_of_file(files[path][i][0])
                        b, pre2 = number_of_file(entry[1])
                        
                        # if they don't have the same prefix, they are different measurements
                        if pre1 != pre2:
                            continue
                        
                        # if the already entered file is newer, don't enter the current file
                        elif a > b:
                            break
                        
                        # if the current file is newer than the entered file
                        elif a < b:
                            # remove the older file
                            files[path].remove(files[path][i])
                            
                            # add current file if not already entered
                            if not added:
                                files[path].append(entry[1:])
                                added = True
                                
                        # if both measurements are the same file
                        elif a == b:
                            files[path].append(entry[1:])
                            break
        
        # enter the files for every path
        for key in files.keys():
            for entry in files[key]:
                new_data.append(entry[1:])
    
    # if duplicates should be included
    else:
        for d in data:
            new_data.append(d[2:])
    
    # if units exist, add them
    if clear_text:
        for i in range(len(columns)):
            column = columns[i]
            # get the name of the column in the database
            column_name = SHORTAGE[messtyp] + column
            if column == "Messgrund":
                column_name = "mgMessgrund"
            elif column == "Sachnummer":
                column_name = "snSachnummer"
            elif column == "Testfeld":
                column_name = "riTestfeld"
            elif column == "Riegelnummer":
                column_name = "riRiegelnummer"
            elif column == "Chipnummer":
                column_name = "diChipNr"
            elif column == "Coating":
                column_name = "chCoatNr"
            elif column == "Messplatz":
                column_name = "mpName"
            elif column == "Header":
                column_name = "hnHeaderNr"
            elif column == "Los":
                column_name = "diAuftrag_ID"
            elif column == "Charge":
                column_name = "chChargenNr"
            elif column == "Dioden-ID":
                column_name = "meDiode_ID"
            if not cleartext[column_name] == "":
                columns[i] = cleartext[column_name]
    
    # create dataframe
    df = create_df(new_data, columns)
    
    # give back data
    return filter_table_re(df, refilters)

#------------------------------------------------------------------------------
#------------------------------------------------------------------------------

def table_charge(charge, messtyp, columns, filters = [], refilters = [], clear_text = True, 
                 sort_out_duplicates = False, auto_filtering = False):
    """
    This function searches the database for measurements of the given charge 
    and messtyp and gives back the given columns. 
    
    example:
        table_charge("C2952-6-3", "Spektrum", ["Diode_ID", "Temperatur", "IatPopt"])
    """
    
    # at least one column has to be given
    assert len(columns) > 0
    
    # setting up the string for the query
    select_str = "SELECT %sPfad_ID, %s, meDiode_ID" % (SHORTAGE[messtyp], FILENAME[messtyp])
    for column in columns:
        if column == "Messgrund":
            select_str += ", mgMessgrund"
        elif column == "Sachnummer":
            select_str += ", snSachnummer"
        elif column == "Testfeld":
            select_str += ", riTestfeld"
        elif column == "Riegelnummer":
            select_str += ", riRiegelnummer"
        elif column == "Chipnummer":
            select_str += ", diChipNr"
        elif column == "Coating":
            select_str += ", chCoatNr"
        elif column == "Messplatz":
            select_str += ", mpName"
        elif column == "Header":
            select_str += ", hnHeaderNr"
        elif column == "Los":
            select_str += ", diAuftrag_ID"
        elif column == "Dioden-ID":
            select_str += ", meDiode_ID"
        elif column == "Charge":
            select_str += ", ChChargenNr"#chChargenNr
        elif column == "ProzessNr":
            select_str += ", chProzessNr"
            
        else:
            select_str += ", %s%s" % (SHORTAGE[messtyp], column)
    select_str += " \n"
    from_str  =  "FROM %s\n" % TABLE[messtyp]
    join_str  =  "INNER JOIN M_Messungen ON %sMeta_ID = meID \n" % SHORTAGE[messtyp]
    join_str  += "INNER JOIN M_Messgrund ON meMessgrund_ID = mgID \n"
    join_str +=  "INNER JOIN P_Diode ON meDiode_ID = diID \n"
    join_str +=  "INNER JOIN P_Riegelsachnummer ON diRiegelSachnummer_ID = risnID \n"
    join_str +=  "INNER JOIN P_Riegel ON risnRiegel_ID = riID \n"
    join_str +=  "INNER JOIN P_Charge ON riCharge_ID = chID \n"
    join_str +=  "INNER JOIN M_MpHwSwKonf ON meMpHwSwKonf_Id = mhsID \n"
    join_str +=  "INNER JOIN M_Messplatz ON mhsMessplatz_ID = mpID \n"
    
    if "Header" in columns:
        join_str +=  "INNER JOIN P_HeaderNr ON meDiode_ID = hnDiode_ID \n"
        
    
    # only join P_Auftrag when necessary, to not exclude diodes without headers
    if "Sachnummer" in columns:
        join_str +=  "INNER JOIN P_Auftrag ON diAuftrag_ID = auID \n"
        join_str +=  "INNER JOIN DB_Sachnummer ON auSachnummer_ID = snID \n"
    
    where_str =  "WHERE meMesstyp_ID = " + MESSTYP_ID[messtyp]
    
    # if only one is given
    if type(charge) == type(""):
        where_str += " AND chChargenNr = '%s' " % charge
    
    # otherwise
    else:
        where_str += " AND (chChargenNr = '%s'" % charge[0]
        for c in charge[1:]:
            where_str += " OR chChargenNr = '%s'" % c
        where_str += ")"
    
    # if auto filtering is chosen
    if auto_filtering:
        for (column, f) in auto_filters[messtyp]:
            if not column in [f[0] for f in filters]:
                filters.append((column, f))
    
    # if filters are given or generated by auto filtering
    if not filters == []:
        where_str += where_str_from_filters(filters, messtyp)
    
    # adding basic strings together
    sql_str = select_str + from_str + join_str + where_str
    
    # setting up connection
    conn = pyodbc.connect('DRIVER={SQL Server};'
                          'SERVER=sql01.eyp.local;'
                          'Trusted_Connection=yes;')
    cursor = conn.cursor()
    
    # executing query
    cursor.execute(sql_str)
    
    # saving results
    data = cursor.fetchall()
    
    # set up data for dataframe
    new_data = []
    
    # if duplicates should be sorted out
    if sort_out_duplicates:
        
        # If measurements have been conducted multiple times, only the last one 
        # will be given back. 
        # Since the measurements will be saved in the same path with almost the 
        # same name, a dictionary saving all measurements of a path is set up here.
        files = {}
        for entry in data:
            
            # only note .dat files
            if entry[1].endswith('.dat') or (entry[1].endswith('.tif') and messtyp == "Strahlprofil") or (entry[1].endswith('.txt') and messtyp == "Per"):
                
                # the path-ID is the first entry in the data list
                path = entry[0]
                
                # if no files in the same path have been entered yet
                if not path in files.keys():
                    # the path file is already the index so it won't be saved into the dictionary
                    files[path] = [entry[1:]]
                
                # in case there have been files from the same path entered into the dictionary
                else:
                    # control variable to check whether current file has been entered yet
                    added = False
                    
                    # compare every file that has been entered into the same path
                    for i in range(len(files[path])):
                        
                        # get the number at the end of the filename and the prefix of both files
                        a, pre1 = number_of_file(files[path][i][0])
                        b, pre2 = number_of_file(entry[1])
                        
                        # if they don't have the same prefix, they are different measurements
                        if pre1 != pre2:
                            continue
                        
                        # if the already entered file is newer, don't enter the current file
                        elif a > b:
                            break
                        
                        # if the current file is newer than the entered file
                        elif a < b:
                            # remove the older file
                            files[path].remove(files[path][i])
                            
                            # add current file if not already entered
                            if not added:
                                files[path].append(entry[1:])
                                added = True
                                
                        # if both measurements are the same file
                        elif a == b:
                            files[path].append(entry[1:])
                            break
        
        # enter the files for every path
        for key in files.keys():
            for entry in files[key]:
                new_data.append(entry[1:])
    
    # if duplicates should be included
    else:
        for d in data:
            new_data.append(d[2:])
    
    # if units exist, add them
    if clear_text:
        for i in range(len(columns)):
            column = columns[i]
            # get the name of the column in the database
            column_name = SHORTAGE[messtyp] + column
            if column == "Messgrund":
                column_name = "mgMessgrund"
            elif column == "Sachnummer":
                column_name = "snSachnummer"
            elif column == "Testfeld":
                column_name = "riTestfeld"
            elif column == "Riegelnummer":
                column_name = "riRiegelnummer"
            elif column == "Chipnummer":
                column_name = "diChipNr"
            elif column == "Coating":
                column_name = "chCoatNr"
            elif column == "Messplatz":
                column_name = "mpName"
            elif column == "Header":
                column_name = "hnHeaderNr"
            elif column == "Los":
                column_name = "diAuftrag_ID"
            elif column == "Charge":
                column_name = "chChargenNr"
            elif column == "Dioden-ID":
                column_name = "chCoatNr"
           
            if not cleartext[column_name] == "":
                columns[i] = cleartext[column_name]
    
    # create dataframe
    df = create_df(new_data, columns)
    
    # give back data
    return filter_table_re(df, refilters)
#------------------------------------------------------------------------------
#------------------------------------------------------------------------------
def table_header(header, messtyp, columns, filters = [], refilters = [], clear_text = True, 
                 sort_out_duplicates = False, auto_filtering = False):
    """
    

    Parameters
    ----------
    head : TYPE
        DESCRIPTION.
    messtyp : TYPE
        DESCRIPTION.
    columns : TYPE
        DESCRIPTION.
    filters : TYPE, optional
        DESCRIPTION. The default is [].
    refilters : TYPE, optional
        DESCRIPTION. The default is [].
    clear_text : TYPE, optional
        DESCRIPTION. The default is True.
    sort_out_duplicates : TYPE, optional
        DESCRIPTION. The default is False.
    auto_filtering : TYPE, optional
        DESCRIPTION. The default is False.

    Returns
    -------
    None.

    """
    # at least one column has to be given
    assert len(columns) > 0
    
    # setting up the string for the query
    select_str = "SELECT %sPfad_ID, %s, meDiode_ID" % (SHORTAGE[messtyp], FILENAME[messtyp])
    for column in columns:
        if column == "Messgrund":
            select_str += ", mgMessgrund"
        elif column == "Sachnummer":
            select_str += ", snSachnummer"
        elif column == "Testfeld":
            select_str += ", riTestfeld"
        elif column == "Riegelnummer":
            select_str += ", riRiegelnummer"
        elif column == "Chipnummer":
            select_str += ", diChipNr"
        elif column == "Coating":
            select_str += ", chCoatNr"
        elif column == "Messplatz":
            select_str += ", mpName"
        elif column == "Header":
            select_str += ", hnHeaderNr"
        elif column == "Los":
            select_str += ", diAuftrag_ID"
        elif column == "Dioden-ID":
            select_str += ", meDiode_ID"
        elif column == "Charge":
            select_str += ", chChargenNr"#", chProzessNr " 
        elif column =="ProzessNr":
            select_str += ", chProzessNr"
       
        else:
            select_str += ", %s%s" % (SHORTAGE[messtyp], column)
    select_str += " \n"
    from_str  =  "FROM %s\n" % TABLE[messtyp]
    join_str  =  "INNER JOIN M_Messungen ON %sMeta_ID = meID \n" % SHORTAGE[messtyp]
    join_str  += "INNER JOIN M_Messgrund ON meMessgrund_ID = mgID \n"
    join_str +=  "INNER JOIN P_HeaderNr ON meDiode_ID = hnDiode_ID \n"
    join_str +=  "INNER JOIN P_Diode ON meDiode_ID = diID \n"
    join_str +=  "INNER JOIN P_Riegelsachnummer ON diRiegelSachnummer_ID = risnID \n"
    join_str +=  "INNER JOIN P_Riegel ON risnRiegel_ID = riID \n"
    join_str +=  "INNER JOIN P_Charge ON riCharge_ID = chID \n"
    join_str +=  "INNER JOIN M_MpHwSwKonf ON meMpHwSwKonf_Id = mhsID \n"
    join_str +=  "INNER JOIN M_Messplatz ON mhsMessplatz_ID = mpID \n"
    join_str +=  "INNER JOIN P_Auftrag ON diAuftrag_ID = auID \n"
    join_str +=  "INNER JOIN DB_Sachnummer ON auSachnummer_ID = snID \n"
    
    where_str =  "WHERE meMesstyp_ID = " + MESSTYP_ID[messtyp]
    
    # if only one is given
    if type(header) == type(""):
        where_str += " AND hnHeaderNr = '%s' " % header
    
    # otherwise
    else:
        where_str += " AND (hnHeaderNr = '%s'" % header[0]
        for h in header[1:]:
            where_str += " OR hnHeaderNr = '%s'" % h
        where_str += ")"
    
    # if auto filtering is chosen
    if auto_filtering:
        for (column, f) in auto_filters[messtyp]:
            if not column in [f[0] for f in filters]:
                filters.append((column, f))
    
    # if filters are given or generated by auto filtering
    if not filters == []:
        where_str += where_str_from_filters(filters, messtyp)
    
    # adding basic strings together
    sql_str = select_str + from_str + join_str + where_str
    
    # setting up connection
    conn = pyodbc.connect('DRIVER={SQL Server};'
                          'SERVER=sql01.eyp.local;'
                          'Trusted_Connection=yes;')
    cursor = conn.cursor()
    
    # executing query
    cursor.execute(sql_str)
    
    # saving results
    data = cursor.fetchall()
    
    # set up data for dataframe
    new_data = []
    
    # if duplicates should be sorted out
    if sort_out_duplicates:
        
        # If measurements have been conducted multiple times, only the last one 
        # will be given back. 
        # Since the measurements will be saved in the same path with almost the 
        # same name, a dictionary saving all measurements of a path is set up here.
        files = {}
        for entry in data:
            
            # only note .dat files
            if entry[1].endswith('.dat') or (entry[1].endswith('.tif') and messtyp == "Strahlprofil") or (entry[1].endswith('.txt') and messtyp == "Per"):
                
                # the path-ID is the first entry in the data list
                path = entry[0]
                
                # if no files in the same path have been entered yet
                if not path in files.keys():
                    # the path file is already the index so it won't be saved into the dictionary
                    files[path] = [entry[1:]]
                
                # in case there have been files from the same path entered into the dictionary
                else:
                    # control variable to check whether current file has been entered yet
                    added = False
                    
                    # compare every file that has been entered into the same path
                    for i in range(len(files[path])):
                        
                        # get the number at the end of the filename and the prefix of both files
                        a, pre1 = number_of_file(files[path][i][0])
                        b, pre2 = number_of_file(entry[1])
                        
                        # if they don't have the same prefix, they are different measurements
                        if pre1 != pre2:
                            continue
                        
                        # if the already entered file is newer, don't enter the current file
                        elif a > b:
                            break
                        
                        # if the current file is newer than the entered file
                        elif a < b:
                            # remove the older file
                            files[path].remove(files[path][i])
                            
                            # add current file if not already entered
                            if not added:
                                files[path].append(entry[1:])
                                added = True
                                
                        # if both measurements are the same file
                        elif a == b:
                            files[path].append(entry[1:])
                            break
        
        # enter the files for every path
        for key in files.keys():
            for entry in files[key]:
                new_data.append(entry[1:])
    
    # if duplicates should be included
    else:
        for d in data:
            new_data.append(d[2:])
    
    # if units exist, add them
    if clear_text:
        for i in range(len(columns)):
            column = columns[i]
            # get the name of the column in the database
            column_name = SHORTAGE[messtyp] + column
            if column == "Messgrund":
                column_name = "mgMessgrund"
            elif column == "Sachnummer":
                column_name = "snSachnummer"
            elif column == "Testfeld":
                column_name = "riTestfeld"
            elif column == "Riegelnummer":
                column_name = "riRiegelnummer"
            elif column == "Chipnummer":
                column_name = "diChipNr"
            elif column == "Coating":
                column_name = "chCoatNr"
            elif column == "Messplatz":
                column_name = "mpName"
            elif column == "Header":
                column_name = "hnHeaderNr"
            elif column == "Los":
                column_name = "diAuftrag_ID"
            elif column == "Charge":
                column_name = "chChargenNr"
            elif column == "Dioden-ID":
                column_name = "meDiode_ID"
            if not cleartext[column_name] == "":
                columns[i] = cleartext[column_name]
    
    # create dataframe
    df = create_df(new_data, columns)
    
    # give back data
    return filter_table_re(df, refilters)
#------------------------------------------------------------------------------
#------------------------------------------------------------------------------
def table_fehlercode(fehler, messtyp, columns, filters = [], refilters = [], clear_text = True, 
                 sort_out_duplicates = False, auto_filtering = False):
    """
    

    Parameters
    ----------
    head : TYPE
        DESCRIPTION.
    messtyp : TYPE
        DESCRIPTION.
    columns : TYPE
        DESCRIPTION.
    filters : TYPE, optional
        DESCRIPTION. The default is [].
    refilters : TYPE, optional
        DESCRIPTION. The default is [].
    clear_text : TYPE, optional
        DESCRIPTION. The default is True.
    sort_out_duplicates : TYPE, optional
        DESCRIPTION. The default is False.
    auto_filtering : TYPE, optional
        DESCRIPTION. The default is False.

    Returns
    -------
    None.

    """
    # at least one column has to be given
    assert len(columns) > 0
    
    # setting up the string for the query
    select_str = "SELECT %sPfad_ID, %s, meDiode_ID" % (SHORTAGE[messtyp], FILENAME[messtyp])
    for column in columns:
        if column == "Messgrund":
            select_str += ", mgMessgrund"
        elif column == "Sachnummer":
            select_str += ", snSachnummer"
        elif column == "Testfeld":
            select_str += ", riTestfeld"
        elif column == "Riegelnummer":
            select_str += ", riRiegelnummer"
        elif column == "Chipnummer":
            select_str += ", diChipNr"
        elif column == "Coating":
            select_str += ", chCoatNr"
        elif column == "Messplatz":
            select_str += ", mpName"
        elif column == "Header":
            select_str += ", hnHeaderNr"
        elif column == "Los":
            select_str += ", diAuftrag_ID"
        elif column == "Dioden-ID":
            select_str += ", meDiode_ID"
        elif column == "Charge":
            select_str += ", chChargenNr"#", chProzessNr " 
        elif column =="ProzessNr":
            select_str += ", chProzessNr"
        elif column == "FehlerCode":
            select_str += ", fcFehlerText"
       
        else:
            select_str += ", %s%s" % (SHORTAGE[messtyp], column)
    select_str += " \n"
    from_str  =  "FROM %s\n" % TABLE[messtyp]
    join_str  =  "INNER JOIN M_Messungen ON %sMeta_ID = meID \n" % SHORTAGE[messtyp]
    join_str  += "INNER JOIN M_Messgrund ON meMessgrund_ID = mgID \n"
    join_str +=  "INNER JOIN P_HeaderNr ON meDiode_ID = hnDiode_ID \n"
    join_str +=  "INNER JOIN P_Diode ON meDiode_ID = diID \n"
    join_str +=  "INNER JOIN P_Riegelsachnummer ON diRiegelSachnummer_ID = risnID \n"
    join_str +=  "INNER JOIN P_Riegel ON risnRiegel_ID = riID \n"
    join_str +=  "INNER JOIN P_Charge ON riCharge_ID = chID \n"
    join_str +=  "INNER JOIN M_MpHwSwKonf ON meMpHwSwKonf_Id = mhsID \n"
    join_str +=  "INNER JOIN M_Messplatz ON mhsMessplatz_ID = mpID \n"
    join_str +=  "INNER JOIN P_Auftrag ON diAuftrag_ID = auID \n"
    join_str +=  "INNER JOIN DB_Sachnummer ON auSachnummer_ID = snID \n"
    join_str += "INNER JOIN P_FehlerCode ON meMessgrund_ID = fcID \n"
    
    where_str =  "WHERE meMesstyp_ID = " + MESSTYP_ID[messtyp]
    
    # if only one is given
    if type(fehler) == type(""):
        where_str += " AND fcFehlerText = '%s' " % fehler
    
    # otherwise
    else:
        where_str += " AND (fcFehlerText = '%s'" % fehler[0]
        for f in fehler[1:]:
            where_str += " OR fcFehlerText = '%s'" % f
        where_str += ")"
    
    # if auto filtering is chosen
    if auto_filtering:
        for (column, f) in auto_filters[messtyp]:
            if not column in [f[0] for f in filters]:
                filters.append((column, f))
    
    # if filters are given or generated by auto filtering
    if not filters == []:
        where_str += where_str_from_filters(filters, messtyp)
    
    # adding basic strings together
    sql_str = select_str + from_str + join_str + where_str
    
    # setting up connection
    conn = pyodbc.connect('DRIVER={SQL Server};'
                          'SERVER=sql01.eyp.local;'
                          'Trusted_Connection=yes;')
    cursor = conn.cursor()
    
    # executing query
    cursor.execute(sql_str)
    
    # saving results
    data = cursor.fetchall()
    
    # set up data for dataframe
    new_data = []
    
    # if duplicates should be sorted out
    if sort_out_duplicates:
        
        # If measurements have been conducted multiple times, only the last one 
        # will be given back. 
        # Since the measurements will be saved in the same path with almost the 
        # same name, a dictionary saving all measurements of a path is set up here.
        files = {}
        for entry in data:
            
            # only note .dat files
            if entry[1].endswith('.dat') or (entry[1].endswith('.tif') and messtyp == "Strahlprofil") or (entry[1].endswith('.txt') and messtyp == "Per"):
                
                # the path-ID is the first entry in the data list
                path = entry[0]
                
                # if no files in the same path have been entered yet
                if not path in files.keys():
                    # the path file is already the index so it won't be saved into the dictionary
                    files[path] = [entry[1:]]
                
                # in case there have been files from the same path entered into the dictionary
                else:
                    # control variable to check whether current file has been entered yet
                    added = False
                    
                    # compare every file that has been entered into the same path
                    for i in range(len(files[path])):
                        
                        # get the number at the end of the filename and the prefix of both files
                        a, pre1 = number_of_file(files[path][i][0])
                        b, pre2 = number_of_file(entry[1])
                        
                        # if they don't have the same prefix, they are different measurements
                        if pre1 != pre2:
                            continue
                        
                        # if the already entered file is newer, don't enter the current file
                        elif a > b:
                            break
                        
                        # if the current file is newer than the entered file
                        elif a < b:
                            # remove the older file
                            files[path].remove(files[path][i])
                            
                            # add current file if not already entered
                            if not added:
                                files[path].append(entry[1:])
                                added = True
                                
                        # if both measurements are the same file
                        elif a == b:
                            files[path].append(entry[1:])
                            break
        
        # enter the files for every path
        for key in files.keys():
            for entry in files[key]:
                new_data.append(entry[1:])
    
    # if duplicates should be included
    else:
        for d in data:
            new_data.append(d[2:])
    
    # if units exist, add them
    if clear_text:
        for i in range(len(columns)):
            column = columns[i]
            # get the name of the column in the database
            column_name = SHORTAGE[messtyp] + column
            if column == "Messgrund":
                column_name = "mgMessgrund"
            elif column == "Sachnummer":
                column_name = "snSachnummer"
            elif column == "Testfeld":
                column_name = "riTestfeld"
            elif column == "Riegelnummer":
                column_name = "riRiegelnummer"
            elif column == "Chipnummer":
                column_name = "diChipNr"
            elif column == "Coating":
                column_name = "chCoatNr"
            elif column == "Messplatz":
                column_name = "mpName"
            elif column == "Header":
                column_name = "hnHeaderNr"
            elif column == "Los":
                column_name = "diAuftrag_ID"
            elif column == "Charge":
                column_name = "chChargenNr"
            elif column == "Dioden-ID":
                column_name = "meDiode_ID"
            if not cleartext[column_name] == "":
                columns[i] = cleartext[column_name]
    
    # create dataframe
    df = create_df(new_data, columns)
    
    # give back data
    return filter_table_re(df, refilters)
#------------------------------------------------------------------------------
#------------------------------------------------------------------------------
def table_prozess(prozess, messtyp, columns, filters = [], refilters = [], clear_text = True, 
                 sort_out_duplicates = False, auto_filtering = False):
    """
    

    Returns
    -------
    None.

    """
    # at least one column has to be given
    assert len(columns) > 0
    
    # setting up the string for the query
    select_str = "SELECT %sPfad_ID, %s, meDiode_ID" % (SHORTAGE[messtyp], FILENAME[messtyp])
    for column in columns:
        if column == "Messgrund":
            select_str += ", mgMessgrund"
        elif column == "Sachnummer":
            select_str += ", snSachnummer"
        elif column == "Testfeld":
            select_str += ", riTestfeld"
        elif column == "Riegelnummer":
            select_str += ", riRiegelnummer"
        elif column == "Chipnummer":
            select_str += ", diChipNr"
        elif column == "Coating":
            select_str += ", chCoatNr"
        elif column == "Messplatz":
            select_str += ", mpName"
        elif column == "Header":
            select_str += ", hnHeaderNr"
        elif column == "Los":
            select_str += ", diAuftrag_ID"
        elif column == "Dioden-ID":
            select_str += ", meDiode_ID"
        elif column == "Charge":
            select_str += ", ChChargenNr"#chChargenNr
        elif column == "ProzessNr":
            select_str += ", chProzessNr"
            
        else:
            select_str += ", %s%s" % (SHORTAGE[messtyp], column)
    select_str += " \n"
    from_str  =  "FROM %s\n" % TABLE[messtyp]
    join_str  =  "INNER JOIN M_Messungen ON %sMeta_ID = meID \n" % SHORTAGE[messtyp]
    join_str  += "INNER JOIN M_Messgrund ON meMessgrund_ID = mgID \n"
    join_str +=  "INNER JOIN P_Diode ON meDiode_ID = diID \n"
    join_str +=  "INNER JOIN P_Riegelsachnummer ON diRiegelSachnummer_ID = risnID \n"
    join_str +=  "INNER JOIN P_Riegel ON risnRiegel_ID = riID \n"
    join_str +=  "INNER JOIN P_Charge ON riCharge_ID = chID \n"
    join_str +=  "INNER JOIN M_MpHwSwKonf ON meMpHwSwKonf_Id = mhsID \n"
    join_str +=  "INNER JOIN M_Messplatz ON mhsMessplatz_ID = mpID \n"
    
    if "Header" in columns:
        join_str +=  "INNER JOIN P_HeaderNr ON meDiode_ID = hnDiode_ID \n"
        
    
    # only join P_Auftrag when necessary, to not exclude diodes without headers
    if "Sachnummer" in columns:
        join_str +=  "INNER JOIN P_Auftrag ON diAuftrag_ID = auID \n"
        join_str +=  "INNER JOIN DB_Sachnummer ON auSachnummer_ID = snID \n"
    
    where_str =  "WHERE meMesstyp_ID = " + MESSTYP_ID[messtyp]
    
    # if only one is given
    if type(prozess) == type(""):
        where_str += " AND chProzessNr = '%s' " % prozess
    
    # otherwise
    else:
        where_str += " AND (chProzessNr = '%s' " % prozess[0]
        for p in prozess[1:]:
            where_str += " OR chProzessNr = '%s'" % p
        where_str += ")"
    
    # if auto filtering is chosen
    if auto_filtering:
        for (column, f) in auto_filters[messtyp]:
            if not column in [f[0] for f in filters]:
                filters.append((column, f))
    
    # if filters are given or generated by auto filtering
    if not filters == []:
        where_str += where_str_from_filters(filters, messtyp)
    
    # adding basic strings together
    sql_str = select_str + from_str + join_str + where_str
    
    # setting up connection
    conn = pyodbc.connect('DRIVER={SQL Server};'
                          'SERVER=sql01.eyp.local;'
                          'Trusted_Connection=yes;')
    cursor = conn.cursor()
    
    # executing query
    cursor.execute(sql_str)
    
    # saving results
    data = cursor.fetchall()
    
    # set up data for dataframe
    new_data = []
    
    # if duplicates should be sorted out
    if sort_out_duplicates:
        
        # If measurements have been conducted multiple times, only the last one 
        # will be given back. 
        # Since the measurements will be saved in the same path with almost the 
        # same name, a dictionary saving all measurements of a path is set up here.
        files = {}
        for entry in data:
            
            # only note .dat files
            if entry[1].endswith('.dat') or (entry[1].endswith('.tif') and messtyp == "Strahlprofil") or (entry[1].endswith('.txt') and messtyp == "Per"):
                
                # the path-ID is the first entry in the data list
                path = entry[0]
                
                # if no files in the same path have been entered yet
                if not path in files.keys():
                    # the path file is already the index so it won't be saved into the dictionary
                    files[path] = [entry[1:]]
                
                # in case there have been files from the same path entered into the dictionary
                else:
                    # control variable to check whether current file has been entered yet
                    added = False
                    
                    # compare every file that has been entered into the same path
                    for i in range(len(files[path])):
                        
                        # get the number at the end of the filename and the prefix of both files
                        a, pre1 = number_of_file(files[path][i][0])
                        b, pre2 = number_of_file(entry[1])
                        
                        # if they don't have the same prefix, they are different measurements
                        if pre1 != pre2:
                            continue
                        
                        # if the already entered file is newer, don't enter the current file
                        elif a > b:
                            break
                        
                        # if the current file is newer than the entered file
                        elif a < b:
                            # remove the older file
                            files[path].remove(files[path][i])
                            
                            # add current file if not already entered
                            if not added:
                                files[path].append(entry[1:])
                                added = True
                                
                        # if both measurements are the same file
                        elif a == b:
                            files[path].append(entry[1:])
                            break
        
        # enter the files for every path
        for key in files.keys():
            for entry in files[key]:
                new_data.append(entry[1:])
    
    # if duplicates should be included
    else:
        for d in data:
            new_data.append(d[2:])
    
    # if units exist, add them
    if clear_text:
        for i in range(len(columns)):
            column = columns[i]
            # get the name of the column in the database
            column_name = SHORTAGE[messtyp] + column
            if column == "Messgrund":
                column_name = "mgMessgrund"
            elif column == "Sachnummer":
                column_name = "snSachnummer"
            elif column == "Testfeld":
                column_name = "riTestfeld"
            elif column == "Riegelnummer":
                column_name = "riRiegelnummer"
            elif column == "Chipnummer":
                column_name = "diChipNr"
            elif column == "Coating":
                column_name = "chCoatNr"
            elif column == "Messplatz":
                column_name = "mpName"
            elif column == "Header":
                column_name = "hnHeaderNr"
            elif column == "Los":
                column_name = "diAuftrag_ID"
            elif column == "Charge":
                column_name = "chChargenNr"
            elif column == "Dioden-ID":
                column_name = "chCoatNr"
           
            if not cleartext[column_name] == "":
                columns[i] = cleartext[column_name]
    
    # create dataframe
    df = create_df(new_data, columns)
    
    # give back data
    return filter_table_re(df, refilters)
#------------------------------------------------------------------------------
#------------------------------------------------------------------------------
def table_chargeFuerSachnummer():
    """
    
    Returns
    -------
    None.

    """
    #Windows authentification and connection to the Database
    Server = 'sql01'
    Database = 'eyp'
    Driver = 'SQL Server'
    Database_Con = f'mssql://@{Server}/{Database}?driver={Driver}'


    #Create connection
    engine = create_engine(Database_Con)
    #session=sessionmaker(bind=engine)()
    conn = pyodbc.connect('DRIVER={SQL Server};'
                          'SERVER=sql01.eyp.local;'
                          'Trusted_Connection=yes;')
    df = pandas.read_sql_query("select * chsnSachnummer_ID from [dbo].[P_ChargeFuerSachnummer]", conn)
    return df
#------------------------------------------------------------------------------
#------------------------------------------------------------------------------

def table_lot(lot, messtyp, columns, filters = [], refilters = [], clear_text = True, 
              sort_out_duplicates = False, auto_filtering = False):
    """
    This function searches the database for measurements of the given lot 
    and messtyp and gives back the given columns. 
    
    example:
        table_lot("7513", "Kennlinie", ["Ith", "Slope"])
    """
    
    # at least one column has to be given
    assert len(columns) > 0
    
    # setting up the string for the query
    select_str = "SELECT %sPfad_ID, %s, meDiode_ID" % (SHORTAGE[messtyp], FILENAME[messtyp])
    for column in columns:
        if column == "Messgrund":
            select_str += ", mgMessgrund"
        elif column == "Sachnummer":
            select_str += ", snSachnummer"
        elif column == "Testfeld":
            select_str += ", riTestfeld"
        elif column == "Riegelnummer":
            select_str += ", riRiegelnummer"
        elif column == "Chipnummer":
            select_str += ", diChipNr"
        elif column == "Messplatz":
            select_str += ", mpName"
        elif column == "Header":
            select_str += ", hnHeaderNr"
        elif column == "Los":
            select_str += ", diAuftrag_ID"
        elif column == "Dioden-ID":
            select_str += ", meDiode_ID"
        elif column == "Coating":
            select_str += ", chCoatNr"
        elif column == "Charge":
            select_str += ", chChargenNr"
        elif column == "ProzessNr":
            select_str += ", chProzessNr"
        else:
            select_str += ", %s%s" % (SHORTAGE[messtyp], column)
    select_str += " \n"
    from_str  =  "FROM %s\n" % TABLE[messtyp]
    join_str  =  "INNER JOIN M_Messungen ON %sMeta_ID = meID \n" % SHORTAGE[messtyp]
    join_str  += "INNER JOIN M_Messgrund ON meMessgrund_ID = mgID \n"
    join_str +=  "INNER JOIN P_Diode ON meDiode_ID = diID \n"
    join_str +=  "INNER JOIN P_Riegelsachnummer ON diRiegelSachnummer_ID = risnID \n"
    join_str +=  "INNER JOIN P_Riegel ON risnRiegel_ID = riID \n"
    join_str +=  "INNER JOIN M_MpHwSwKonf ON meMpHwSwKonf_Id = mhsID \n"
    join_str +=  "INNER JOIN P_Charge ON riCharge_ID = chID \n"
    join_str +=  "INNER JOIN M_Messplatz ON mhsMessplatz_ID = mpID \n"
    where_str =  "WHERE meMesstyp_ID = " + MESSTYP_ID[messtyp]
    
    # only join P_Auftrag when necessary, to not exclude diodes without headers
    if "Header" in columns or "Sachnummer" in columns or "Los" in columns:
        join_str +=  "INNER JOIN P_HeaderNr ON meDiode_ID = hnDiode_ID \n"
        join_str +=  "INNER JOIN P_Auftrag ON diAuftrag_ID = auID \n"
        join_str +=  "INNER JOIN DB_Sachnummer ON auSachnummer_ID = snID \n"
    
    # if only one is given
    if type(lot) == type("") or type(lot) == type(0):
        where_str += " AND diAuftrag_ID = '%s' " % lot
    
    # otherwise
    else:
        where_str += " AND (diAuftrag_ID = '%s'" % lot[0]
        for l in lot[1:]:
            where_str += " OR diAuftrag_ID = '%s'" % l
        where_str += ")"
    
    # if auto filtering is chosen
    if auto_filtering:
        for (column, f) in auto_filters[messtyp]:
            if not column in [f[0] for f in filters]:
                filters.append((column, f))
    
    # if filters are given or generated by auto filtering
    if not filters == []:
        where_str += where_str_from_filters(filters, messtyp)
    
    # adding basic strings together
    sql_str = select_str + from_str + join_str + where_str
    
    # setting up connection
    conn = pyodbc.connect('DRIVER={SQL Server};'
                          'SERVER=sql01.eyp.local;'
                          'Trusted_Connection=yes;')
    cursor = conn.cursor()
    
    # executing query
    cursor.execute(sql_str)
    
    # saving results
    data = cursor.fetchall()
    
    # set up data for dataframe
    new_data = []
    
    # if duplicates should be sorted out
    if sort_out_duplicates:
        
        # If measurements have been conducted multiple times, only the last one 
        # will be given back. 
        # Since the measurements will be saved in the same path with almost the 
        # same name, a dictionary saving all measurements of a path is set up here.
        files = {}
        for entry in data:
            
            # only note .dat files
            if entry[1].endswith('.dat') or (entry[1].endswith('.tif') and messtyp == "Strahlprofil") or (entry[1].endswith('.txt') and messtyp == "Per"):
                
                # the path-ID is the first entry in the data list
                path = entry[0]
                
                # if no files in the same path have been entered yet
                if not path in files.keys():
                    # the path file is already the index so it won't be saved into the dictionary
                    files[path] = [entry[1:]]
                
                # in case there have been files from the same path entered into the dictionary
                else:
                    # control variable to check whether current file has been entered yet
                    added = False
                    
                    # compare every file that has been entered into the same path
                    for i in range(len(files[path])):
                        
                        # get the number at the end of the filename and the prefix of both files
                        a, pre1 = number_of_file(files[path][i][0])
                        b, pre2 = number_of_file(entry[1])
                        
                        # if they don't have the same prefix, they are different measurements
                        if pre1 != pre2:
                            continue
                        
                        # if the already entered file is newer, don't enter the current file
                        elif a > b:
                            break
                        
                        # if the current file is newer than the entered file
                        elif a < b:
                            # remove the older file
                            files[path].remove(files[path][i])
                            
                            # add current file if not already entered
                            if not added:
                                files[path].append(entry[1:])
                                added = True
                                
                        # if both measurements are the same file
                        elif a == b:
                            files[path].append(entry[1:])
                            break
        
        # enter the files for every path
        for key in files.keys():
            for entry in files[key]:
                new_data.append(entry[1:])
    
    # if duplicates should be included
    else:
        for d in data:
            new_data.append(d[2:])
    
    # if units exist, add them
    if clear_text:
        for i in range(len(columns)):
            column = columns[i]
            # get the name of the column in the database
            column_name = SHORTAGE[messtyp] + column
            if column == "Messgrund":
                column_name = "mgMessgrund"
            elif column == "Sachnummer":
                column_name = "snSachnummer"
            elif column == "Testfeld":
                column_name = "riTestfeld"
            elif column == "Riegelnummer":
                column_name = "riRiegelnummer"
            elif column == "Chipnummer":
                column_name = "diChipNr"
            elif column == "Coating":
                column_name = "chCoatNr"
            elif column == "Messplatz":
                column_name = "mpName"
            elif column == "Header":
                column_name = "hnHeaderNr"
            elif column == "Los":
                column_name = "diAuftrag_ID"
            elif column == "Charge":
                column_name = "chChargenNr"
            elif column == "Dioden-ID":
                column_name = 'meDiode_ID'
            if not cleartext[column_name] == "":
                columns[i] = cleartext[column_name]
    
    # create the dataframe
    df = create_df(new_data, columns)
    
    # give back filtered dataframe
    return filter_table_re(df, refilters)

#------------------------------------------------------------------------------
#------------------------------------------------------------------------------

def number_of_file(filename):
    """
    This function analyzes the given filename for the number at the end, right
    before the '.dat', and the string before the number. 
    """
    number = 0
    string = ''
    
    # the number can have 1-3 digits :|
    for i in range(len(filename)-5, 0, -1):
        # check if the part from the current digit until the dot is a number
        try:
            number = int(filename[i:-4])
            string = filename[:i]
            
        # stop, if the number and string have been found
        except:
            break
        
    # return the results
    return (number, string)

#------------------------------------------------------------------------------
#------------------------------------------------------------------------------

def create_df(data_list, columns):
    """
    This function converts the generated datalist into a dataframe from the 
    pandas library. 
    """
    
    # seperate the columns
    df_lists = [[] for c in range(len(columns)+1)]
    for item in data_list:
        for i in range(len(columns)+1):
            df_lists[i].append(item[i])
    
    index = df_lists[0]
    df_lists = df_lists[1:]
    
    # create a list of series (another pandas-datatype)
    series_list = [pandas.Series(df_lists[i], index = index) for i in range(len(columns))]
    
    # create dataframe from the series
    df = pandas.concat(series_list, axis = 1)
    
    # name columns
    df.columns = columns
    
    # return result
    return df

#------------------------------------------------------------------------------
#------------------------------------------------------------------------------

def filter_table_re(table, characteristics):
    """
    This funtcion applys given regular expression filters to given columns in 
    the given dataframe. 
    
    example:
        filter_table_re(table, [("Messgrund", "Messung")])
    """
    for (column, regex) in characteristics:
        table = table[table[column].str.match(regex)]
    return table

#------------------------------------------------------------------------------
#------------------------------------------------------------------------------

def export_excel(table, filename):
    """
    Exports the given table into an excel file. filename has to end with '.xlsx'.
    """
    writer = pandas.ExcelWriter(filename, engine = "xlsxwriter")
    
    # copy table, so the original won't be changed in the following step
    table = table.copy()
    
    # remove non-ascii characters from the column names
    table.columns = [re.sub(r'[^\x00-\x7F]+','?', i) for i in table.columns]
    
    try:
        # remove non-ascii characters from content
        table.replace({r'[^\x00-\x7F]+':'?'}, regex=True, inplace=True)
    except:
        pass
    
    # export
    table.to_excel(writer, sheet_name = "Tabelle1")
    
    return writer

#------------------------------------------------------------------------------
#------------------------------------------------------------------------------

def import_excel(filename):
    """
    Imports table from excel file.
    """
    return pandas.read_excel(filename)

#------------------------------------------------------------------------------
#------------------------------------------------------------------------------

def percentile(iterable, percentage):
    """
    Calculates the statistical percentile with the given percentage (the value 
    below which <percentage>% of the values are). 
    """
    return iterable[int(len(iterable) * percentage)]

#------------------------------------------------------------------------------
#------------------------------------------------------------------------------

def mode(iterable):
    """
    Calculates the statistical mode (the most common value). 
    """
    
    # remember count for every value 
    count = {}
    for i in iterable:
        if not i in count.keys():
            count[i] = 1
        else:
            count[i] += 1
    
    keys = list(count.keys())
    # determine maximum
    maximum = keys[0]
    for c in keys[1:]:
        if count[c] > count[maximum]:
            maximum = c
    
    # give it back
    return maximum

#------------------------------------------------------------------------------
#------------------------------------------------------------------------------

def amean(iterable):
    """
    Calculates the arithmetic mean. 
    """
    try:
        return sum(iterable) / len(iterable)
    except:
        return ""

#------------------------------------------------------------------------------
#------------------------------------------------------------------------------

def stdev(iterable):
    """
    Calculates the standard deviation. 
    """
    try:
        a = amean(iterable)
        s = sum([(a-i) ** 2 for i in iterable]) / len(iterable)
        return math.sqrt(s)
    except:
        return ""

#------------------------------------------------------------------------------
#------------------------------------------------------------------------------

def skewness(iterable):
    """
    Calculates the skewness. 
    """
    try:
        a = amean(iterable)
        s = sum([((a-i)/stdev(iterable)) ** 3 for i in iterable]) / len(iterable)
        return s
    except:
        return ""

#------------------------------------------------------------------------------
#------------------------------------------------------------------------------

def kurtosis(iterable):
    """
    Calculates the kurtosis. 
    """
    try:
        a = amean(iterable)
        s = sum([((a-i)/stdev(iterable)) ** 4 for i in iterable]) / len(iterable)
        return s
    except:
        return ""
