from pytest import approx



tests = {
        'gi-multi' : {
                'src_dir'  : '/dls/i04/data/2013/nt5966-4/20131007/GI/P2_X3',
                'src_run_num'   : (2,3,4,5,6,7,8),
                'src_prefix' : ('GI_M2S3',), 
                'results': {'a' : approx(100,abs=100), 'b' : approx(100,abs=100), 'c' : approx(100,abs=100), 
                            'alpha' : approx(180, abs=180), 'beta' : approx(180, abs=180), 'gamma' : approx(180, abs=180)}
                     },

        'gridscan': {
                'src_dir'  : '/dls/i04/data/2014/cm4952-4/20141015/grid/th1_2',
                'src_run_num'   : (1,),
                'src_prefix' : ('grid1',),
                'results': { }
                    },
   
        'gridscan2': {
                'src'    : '/dls/i04/data/2016/cm14452-2/20160425/grid/protein-1-cm14452-2/thermo_10',
                'src_run_num'   : (3,),
                'src_prefix' : ('thermo_10',),
                'results': { }
                     },

       'high-multiplicity': {
                'src'    : '/dls/i03/data/2012/cm5698-4/therm2',
                'src_run_num'   : (1,),
                'src_prefix' : ('thermc',),
                'results': {'a' : approx(100,abs=100), 'b' : approx(100,abs=100), 'c' : approx(100,abs=100), 
                            'alpha' : approx(180, abs=180), 'beta' : approx(180, abs=180), 'gamma' : approx(180, abs=180)}
                     },

       'example-i04': {
                'src'    : '/dls/i04/data/2017/cm16781-4/20171015/Thaum/Th_3',
                'src_run_num'   : (1,),
                'src_prefix' : ('Th_3',),
                'debug' : True,
                'use_sample_id' : 790046,
                'results': {'a' : approx(100,abs=100), 'b' : approx(100,abs=100), 'c' : approx(100,abs=100), 
                            'alpha' : approx(180, abs=180), 'beta' : approx(180, abs=180), 'gamma' : approx(180, abs=180)}
                      },

       'example-i23': {
                'src'    : '/dls/i23/data/2017/cm16790-4/20171012/germanate_4p5keV',
                'src_run_num'   : (1,),
                'src_prefix' : ('data_A',),
                'debug' : True,
                'use_sample_id' : 1172864,
                'results': {'a' : approx(100,abs=100), 'b' : approx(100,abs=100), 'c' : approx(100,abs=100), 
                            'alpha' : approx(180, abs=180), 'beta' : approx(180, abs=180), 'gamma' : approx(180, abs=180)}
                      },

       'example-ins': {
                'src'    : '/dls/i03/data/2016/cm14451-4/gw/20161003/ins/INS2',
                'src_run_num'   : (2,),
                'src_prefix' : ('INS2',),
                'debug' : True,
                'results': {'a' : approx(100,abs=100), 'b' : approx(100,abs=100), 'c' : approx(100,abs=100), 
                            'alpha' : approx(180, abs=180), 'beta' : approx(180, abs=180), 'gamma' : approx(180, abs=180)}
                      },

       'example-ins-2': {
                'src'    : '/dls/i03/data/2016/cm14451-4/gw/20161003/ins/INS2',
                'src_run_num'   : (2,),
                'src_prefix' : ('INS2',),
                'debug' : True,
                'use_sample_id' : 434837,
                'results': {'a' : approx(100,abs=100), 'b' : approx(100,abs=100), 'c' : approx(100,abs=100), 
                            'alpha' : approx(180, abs=180), 'beta' : approx(180, abs=180), 'gamma' : approx(180, abs=180)}
                        },

       'mad-multi': {
                'src'    : '/dls/i04/data/2017/cm16781-1/20170111/ZnMAD',
                'src_run_num'   : (1,),
                'src_prefix' : ('sp0092_Zn_pk', 'sp0092_Zn_if', 'sp0092_Zn_hrm'),
                'results': {'a' : approx(100,abs=100), 'b' : approx(100,abs=100), 'c' : approx(100,abs=100), 
                            'alpha' : approx(180, abs=180), 'beta' : approx(180, abs=180), 'gamma' : approx(180, abs=180)}
                    },
       
       'native': {
                'src'    : '/dls/i03/data/2017/cm16791-1/20170221/gw/20170221/INS2',
                'src_run_num'   : (1,),
                'src_prefix' : ('INS2_29_2',),
                'debug' : True,
                'use_sample_id' : 787559,
                'results': {'a' : approx(100,abs=100), 'b' : approx(100,abs=100), 'c' : approx(100,abs=100), 
                            'alpha' : approx(180, abs=180), 'beta' : approx(180, abs=180), 'gamma' : approx(180, abs=180)}
  
                 },

        'protk-au-insitu': {
                 'src'    : '/dls/i24/data/2017/nr16818-47/Josh/InSitu/CrystalQuickX/ProtK/Au',
                 'src_run_num'   : (1,),
                 'src_prefix' : ('ProtK_InSitu_Au_47_1','ProtK_InSitu_Au_47_2','ProtK_InSitu_Au_47_3','ProtK_InSitu_Au_47_4','ProtK_InSitu_Au_47_5'
                                 'ProtK_InSitu_Au_47_6','ProtK_InSitu_Au_47_7','ProtK_InSitu_Au_47_8','ProtK_InSitu_Au_47_9','ProtK_InSitu_Au_47_10'),
                 'results': {'a' : approx(100,abs=100), 'b' : approx(100,abs=100), 'c' : approx(100,abs=100), 
                             'alpha' : approx(180, abs=180), 'beta' : approx(180, abs=180), 'gamma' : approx(180, abs=180)}
                 },

        'example-sad': {
                 'src'    : '/dls/i02/data/2013/nt5964-1/2013_02_08/GW/DNA/P1/X1',
                 'src_run_num'   : (1,),
                 'src_prefix' : ('X1_weak_M1S1',),
                 'results': {'a' : approx(100,abs=100), 'b' : approx(100,abs=100), 'c' : approx(100,abs=100), 
                             'alpha' : approx(180, abs=180), 'beta' : approx(180, abs=180), 'gamma' : approx(180, abs=180)}
                 },
         
        'example-sad-multi': {
                 'src'    : '/dls/i02/data/2013/nt5964-1/2013_02_08/GW/DNA/P1/X1',
                 'src_run_num'   : (1,3),
                 'src_prefix' : ('X1_strong_M1S1',),
                 'results': {'a' : approx(100,abs=100), 'b' : approx(100,abs=100), 'c' : approx(100,abs=100), 
                             'alpha' : approx(180, abs=180), 'beta' : approx(180, abs=180), 'gamma' : approx(180, abs=180)}
                 },

        'screening': {
                 'src'    : '/dls/i04/data/2017/cm16781-1/20170223/group1/Thaum/Th_4',
                 'src_run_num'   : (1,),
                 'src_prefix' : ('Th_4',),
                 'use_sample_id' : 790048,
                 'debug' : True,
                 'results': {'a' : approx(100,abs=100), 'b' : approx(100,abs=100), 'c' : approx(100,abs=100), 
                             'alpha' : approx(180, abs=180), 'beta' : approx(180, abs=180), 'gamma' : approx(180, abs=180)}
                 },
        'screening-i24': {
                 'src'    : '/dls/i24/data/2017/cm16788-3/screening/hewl/hewl_1',
                 'src_run_num'   : (1,),
                 'src_prefix' : ('hewl_1',),
                 'use_sample_id' : 1018393,
                 'debug' : True,
                 'results': {'a' : approx(100,abs=100), 'b' : approx(100,abs=100), 'c' : approx(100,abs=100), 
                             'alpha' : approx(180, abs=180), 'beta' : approx(180, abs=180), 'gamma' : approx(180, abs=180)}
                 },

        'screening-smargon': {
                 'src'    : '/dls/i04/data/2017/cm16781-1/20170316/Thaum/Th_4',
                 'src_run_num'   : (2,),
                 'src_prefix' : ('Th_4',),
                 'use_sample_id' : 790048,
                 'debug' : True,
                 'results': {'a' : approx(100,abs=100), 'b' : approx(100,abs=100), 'c' : approx(100,abs=100), 
                             'alpha' : approx(180, abs=180), 'beta' : approx(180, abs=180), 'gamma' : approx(180, abs=180)}
                 }

                    

        }
