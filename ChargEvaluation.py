# -*- coding: utf-8 -*-
"""
Created on Thu Oct 10 10:00:03 2024

@author: danmop
"""

#import pandas as pd
import eypMessdaten as eypDB


class ch():
    """Class for evaluation of chip (Diode) properties in Wafer after
    integration."""

#------------------------------------------------------------------------------

    def __init__(self, ch='', messgrund1='', messgrund2=''):
        """
        Initilize 

        Parameters
        ----------
        ch : str
            Charge Number
        messgrund1 : str
            Messgrund in
        messgrund2 : str
            Messgrund out

        Returns
        -------
        None.

        """
        self.ch = ch
        self.messgrund1 = messgrund1
        self.messgrund2 = messgrund2

#------------------------------------------------------------------------------
    
    def __str__(self):
        """
        String representation

        Returns
        -------
        String representation.

        """
        
        return f'Charge: {self.ch} ({self.messgrund1} -> {self.messgrund2})'

#------------------------------------------------------------------------------
    
    def get_kl_data_eypdb(self):
        """
        Fetches measurement data 'Kennlinie' data base.

        Parameters
        ----------
        None.

        Returns
        -------
        None.

        """
        
        # Kennlinie
        columns_kl = ['Messgrund',
                      'Sachnummer',
                      'Charge',
                      'ProzessNr',
                      'Coating',
                      'Testfeld',
                      'Riegelnummer',
                      'Chipnummer',
                      'Dioden-ID',
                      'Los',
                      'Datum',
                      'Temperatur',
                      'Popt',
                      'IatPopt',
                      'Slope',
                      'Ith',
                      'Messdatei',
                      'Header',
                      'FehlerCode'
                    
                      ]
        
        df_kl = eypDB.table_charge(self.ch,'Kennlinie',columns_kl,
                               sort_out_duplicates = True)
        
        df_kl = df_kl.rename(columns={'Datum.Messdatum':  'Datum',
                                      'in °C': 'Temperatur.in °c_kl',
                                      'in mW':       'Popt.in mW',
                                      'in mA':    'IatPopt.in mA',
                                      'in W/A':     'Slope.in W/A_kl',
                                      ' in mA':        'Ith.in mA',
                                      'FehlerCode':       'FehlerText'
                                    
                                    })
                
        return df_kl
    

#------------------------------------------------------------------------------

    def get_sp_data_eypdb(self):
        """
        Fetches measurement data 'Spektrum' data base.

        Parameters
        ----------
        None.

        Returns
        -------
        None.

        """

        # Spektrum
        columns_sp = ['Messgrund',
                      'Sachnummer',
                      'Charge',
                      'ProzessNr',
                      'Coating',
                      'Testfeld',
                      'Riegelnummer',
                      'Chipnummer',
                      'Dioden-ID',
                      'Los',
                      'Datum',
                      'Temperatur',
                      'Popt',
                      'I',
                      'Wavelength',
                      'Messdatei',
                      'Header',
                      'FehlerCode',
                      'Smsr']
        
        df_sp = eypDB.table_charge(self.ch,'Spektrum',columns_sp,
                               sort_out_duplicates = True)
        
        df_sp = df_sp.rename(columns={'Datum.Messdatum':  'spDatum',
                                      'in °C': 'Temperatur.in °C_sp',
                                      'in mW':       'Popt.in mW',
                                      'in mA':          'IatPopt.in mA',
                                      'in nm': 'Wavelength.in nm_sp',
                                      'FehlerCode':       'FehlerText',
                                      'in dB': 'Smsr.in dB_sp'})
        
        return df_sp
     