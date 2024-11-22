# -*- coding: utf-8 -*-
"""
Created on Thu Oct 10 15:25:20 2024

@author: danmop
"""

import subprocess
import ChargEvaluation as ch
import pandas as pd
import numpy as np

###############################################################################
charge = 'B0294-6-1'
messgrund1 = 'MessungVorPackaging'
messgrund2 = 'Messung2'

###########################################
def main():
    """
    Test with ChargeNr
    """
    ch_dfb = ch.ch(charge,
               messgrund1=messgrund1,
               messgrund2=messgrund2)
    print(str(ch_dfb))

    print('=== FETCH DATA ===')
    df_nach_Charge_kl = ch_dfb.get_kl_data_eypdb()
    df_nach_Charge_sp = ch_dfb.get_sp_data_eypdb()

    """
    makes a merge on Dioden_ID (kl-dataFrame und sp-dataFrame), to get a join of the results from the two measuments.
    suffixes _kl and _sp are for Kennlinie and Spectrum measuments
    """
    df_nach_Charge_join = pd.merge(df_nach_Charge_kl, df_nach_Charge_sp, on=['Dioden-ID'], how='outer', suffixes=('_sp', '_kl'))
    df_nach_Charge_join = df_nach_Charge_join.iloc[:, np.lexsort((df_nach_Charge_join.columns.str.endswith('_sp'), ))]
#df_nach_Charge_join= df_nach_Charge_join.isnull()


if __name__ == "__main__":
    main()