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
                'src_dir'  : '/dls/i04/data/2016/cm14452-2/20160425/grid/protein-1-cm14452-2/thermo_10',
                'src_run_num'   : (3,),
                'src_prefix' : ('thermo_10',),
                'results': { }
                     },

       'high-multiplicity': {
                'src_dir'    : '/dls/i03/data/2012/cm5698-4/therm2',
                'src_run_num'   : (1,),
                'src_prefix' : ('thermc',),
                'results': {'a' : approx(100,abs=100), 'b' : approx(100,abs=100), 'c' : approx(100,abs=100), 
                            'alpha' : approx(180, abs=180), 'beta' : approx(180, abs=180), 'gamma' : approx(180, abs=180)}
                     },

       'example-i04': {
                'src_dir'    : '/dls/i04/data/2017/cm16781-4/20171015/Thaum/Th_3',
                'src_run_num'   : (1,),
                'src_prefix' : ('Th_3',),
                'use_sample_id' : 790046,
                'results': {'a' : approx(100,abs=100), 'b' : approx(100,abs=100), 'c' : approx(100,abs=100), 
                            'alpha' : approx(180, abs=180), 'beta' : approx(180, abs=180), 'gamma' : approx(180, abs=180)}
                      },

       'i23-germanate': {
                'src_dir'    : '/dls/i23/data/2017/cm16790-4/20171012/germanate_4p5keV',
                'src_run_num'   : (1,),
                'src_prefix' : ('data_A',),
                'use_sample_id' : 1172864,
                'results': {'a' : approx(100,abs=100), 'b' : approx(100,abs=100), 'c' : approx(100,abs=100), 
                            'alpha' : approx(180, abs=180), 'beta' : approx(180, abs=180), 'gamma' : approx(180, abs=180)}
                      },

       'example-ins': {
                'src_dir'    : '/dls/i03/data/2016/cm14451-4/gw/20161003/ins/INS2',
                'src_run_num'   : (2,),
                'src_prefix' : ('INS2',),
                'results': {'a' : approx(100,abs=100), 'b' : approx(100,abs=100), 'c' : approx(100,abs=100), 
                            'alpha' : approx(180, abs=180), 'beta' : approx(180, abs=180), 'gamma' : approx(180, abs=180)}
                      },

       'example-ins-2': {
                'src_dir'    : '/dls/i03/data/2016/cm14451-4/gw/20161003/ins/INS2',
                'src_run_num'   : (2,),
                'src_prefix' : ('INS2',),
                'use_sample_id' : 434837,
                'results': {'a' : approx(100,abs=100), 'b' : approx(100,abs=100), 'c' : approx(100,abs=100), 
                            'alpha' : approx(180, abs=180), 'beta' : approx(180, abs=180), 'gamma' : approx(180, abs=180)}
                        },

       'mad-multi': {
                'src_dir'    : '/dls/i04/data/2017/cm16781-1/20170111/ZnMAD',
                'src_run_num'   : (1,),
                'src_prefix' : ('sp0092_Zn_pk', 'sp0092_Zn_if', 'sp0092_Zn_hrm'),
                'results': {'a' : approx(100,abs=100), 'b' : approx(100,abs=100), 'c' : approx(100,abs=100), 
                            'alpha' : approx(180, abs=180), 'beta' : approx(180, abs=180), 'gamma' : approx(180, abs=180)}
                    },
       
       'native': {
                'src_dir'    : '/dls/i03/data/2017/cm16791-1/20170221/gw/20170221/INS2',
                'src_run_num'   : (1,),
                'src_prefix' : ('INS2_29_2',),
                'use_sample_id' : 787559,
                'results': {'a' : approx(100,abs=100), 'b' : approx(100,abs=100), 'c' : approx(100,abs=100), 
                            'alpha' : approx(180, abs=180), 'beta' : approx(180, abs=180), 'gamma' : approx(180, abs=180)}
                 },

        'protk-au-insitu': {
                 'src_dir'    : '/dls/i24/data/2017/nr16818-47/Josh/InSitu/CrystalQuickX/ProtK/Au',
                 'src_run_num'   : (1,),
                 'src_prefix' : ('ProtK_InSitu_Au_47_1','ProtK_InSitu_Au_47_2','ProtK_InSitu_Au_47_3','ProtK_InSitu_Au_47_4','ProtK_InSitu_Au_47_5'
                                 'ProtK_InSitu_Au_47_6','ProtK_InSitu_Au_47_7','ProtK_InSitu_Au_47_8','ProtK_InSitu_Au_47_9','ProtK_InSitu_Au_47_10'),
                 'results': {'a' : approx(100,abs=100), 'b' : approx(100,abs=100), 'c' : approx(100,abs=100), 
                             'alpha' : approx(180, abs=180), 'beta' : approx(180, abs=180), 'gamma' : approx(180, abs=180)}
                 },

        'smargon-dcg': {
                 'src_dir'    : '/dls/i03/data/2018/cm19644-4/20180912/chigroup/protk/',
                 'src_run_num'   : (1,),
                 'src_prefix' : ('protk_8', 'protk_9', 'protk_10'),
                 'results': {'a' : approx(100,abs=100), 'b' : approx(100,abs=100), 'c' : approx(100,abs=100),
                             'alpha' : approx(180, abs=180), 'beta' : approx(180, abs=180), 'gamma' : approx(180, abs=180)}
                 },

        'sad': {
                 'src_dir'    : '/dls/i02/data/2013/nt5964-1/2013_02_08/GW/DNA/P1/X1',
                 'src_run_num'   : (1,),
                 'src_prefix' : ('X1_weak_M1S1',),
                 'results': {'a' : approx(100,abs=100), 'b' : approx(100,abs=100), 'c' : approx(100,abs=100),
                             'alpha' : approx(180, abs=180), 'beta' : approx(180, abs=180), 'gamma' : approx(180, abs=180)}
                 },

        'sad-multi': {
                 'src_dir'    : '/dls/i02/data/2013/nt5964-1/2013_02_08/GW/DNA/P1/X1',
                 'src_run_num'   : (1,3),
                 'src_prefix' : ('X1_strong_M1S1',),
                 'results': {'a' : approx(100,abs=100), 'b' : approx(100,abs=100), 'c' : approx(100,abs=100),
                             'alpha' : approx(180, abs=180), 'beta' : approx(180, abs=180), 'gamma' : approx(180, abs=180)}
                 },

        'screening': {
                 'src_dir'    : '/dls/i04/data/2017/cm16781-1/20170223/group1/Thaum/Th_4',
                 'src_run_num'   : (1,),
                 'src_prefix' : ('Th_4',),
                 'use_sample_id' : 790048,
                 'results': {'a' : approx(100,abs=100), 'b' : approx(100,abs=100), 'c' : approx(100,abs=100), 
                             'alpha' : approx(180, abs=180), 'beta' : approx(180, abs=180), 'gamma' : approx(180, abs=180)}
                 },

        'screening-i24': {
                 'src_dir'    : '/dls/i24/data/2017/cm16788-3/screening/hewl/hewl_1',
                 'src_run_num'   : (1,),
                 'src_prefix' : ('hewl_1',),
                 'use_sample_id' : 1018393,
                 'results': {'a' : approx(100,abs=100), 'b' : approx(100,abs=100), 'c' : approx(100,abs=100), 
                             'alpha' : approx(180, abs=180), 'beta' : approx(180, abs=180), 'gamma' : approx(180, abs=180)}
                 },

        'screening-smargon': {
                 'src_dir'    : '/dls/i04/data/2017/cm16781-1/20170316/Thaum/Th_4',
                 'src_run_num'   : (2,),
                 'src_prefix' : ('Th_4',),
                 'use_sample_id' : 790048,
                 'results': {'a' : approx(100,abs=100), 'b' : approx(100,abs=100), 'c' : approx(100,abs=100), 
                             'alpha' : approx(180, abs=180), 'beta' : approx(180, abs=180), 'gamma' : approx(180, abs=180)}
                 },

        'trp-multi': {
                 'src_dir'    : '/dls/i04/data/2013/nt5966-4/20131007/TRP/P1_X6',
                 'src_run_num'   : (1,2,3,4),
                 'src_prefix' : ('TRP_M1S6',),
                 'results': {'a' : approx(100,abs=100), 'b' : approx(100,abs=100), 'c' : approx(100,abs=100), 
                             'alpha' : approx(180, abs=180), 'beta' : approx(180, abs=180), 'gamma' : approx(180, abs=180)}
                 },

        'i04-83': {
                 'src_dir'    : '/dls/i04/data/2017/cm16781-1/20170111/autocollect/sp0092',
                 'src_run_num'   : (1,),
                 'src_prefix' : ('s_4',),
                 'results': {'a' : approx(100,abs=100), 'b' : approx(100,abs=100), 'c' : approx(100,abs=100), 
                             'alpha' : approx(180, abs=180), 'beta' : approx(180, abs=180), 'gamma' : approx(180, abs=180)}
                 },

        'sane-example-big': {
                 'src_dir'    : '/dls/i03/data/2016/cm14451-4/gw/20161003/ins/INS2',
                 'src_run_num'   : (2,),
                 'src_prefix' : ('INS2',),
                 'results': {'a' : approx(100,abs=100), 'b' : approx(100,abs=100), 'c' : approx(100,abs=100), 
                             'alpha' : approx(180, abs=180), 'beta' : approx(180, abs=180), 'gamma' : approx(180, abs=180)}
                 },

        }
