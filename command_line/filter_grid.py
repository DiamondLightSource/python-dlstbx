#!/usr/bin/env python
#
# LIBTBX_SET_DISPATCHER_NAME dials.filter_grid

from dials.array_family import flex
from dials.command_line.find_spots import phil_scope
from dials.util import log
from dials.util.options import OptionParser
from dials.util.options import flatten_datablocks
from functools import partial
from itertools import combinations
from math import ceil, log10
from matplotlib import cm
from scipy import stats, mean
from scipy.stats.stats import chisquare
import libtbx.load_env
import logging
import matplotlib.pyplot as plt
import numpy as np
from collections import OrderedDict
import json
logger = logging.getLogger(libtbx.env.dispatcher_name)

# Set the phil scope
from libtbx.phil import parse
phil_scope = parse('''

    filter_grid {
        scoring = *ks chi2
            .help = "Use either Kolmogorov-Smirnov or Chi^2 goodness-of-fit test"
            .type = choice
        threshold = 0.005
            .help = "Significance level of p-value in  acceptance test"
            .type = float(value_min=0)
        min_spots = 20
            .help = "Minimal number of spots per image"
            .type = int
        sample = 60
            .help = "Number of sampled spots for KS-test or dof(bins) in Chi^2 test"
            .type = int(value_min=1)
        profiles = *expon *chi2_low chi2_high rayleigh gengamma
            .help = "List of used distribution functions"
            .type = choice(multi=True)
        plots = score spots
            .help = "Show histograms with score values and/or # spots per image"
            .type = choice(multi=True)
        profile_plots = []
            .help = "Plot experimental and fitted PDF profiles for individual images"
            .type = ints
        show_all = False
            .help = "Include results for images with the scores below threshold"
            .type = bool
    }
    distribution {
        gengamma {
            shape = 3.0
                .type = float(value_min=0)
                .help = "Scale value used to set parameters c = shape, a = 1 / shape"
                        " in the generalised Gamma distribution"
                        " abs(c) * x**(c*a-1) * exp(-x**c) / gamma(a)"
        }
        chi2_low {
            k = 1.0
                .type = float(value_min=0)
                .help = "k degrees of freedom parameter in the Chi^2 distribution"
        }
        
        chi2_high {
            k = 3.0
                .type = float(value_min=0)
                .help = "k degrees of freedom parameter in the Chi^2 distribution"
        }
    }
    include scope dials.command_line.find_spots.phil_scope

''', process_includes=True)


usage = "%s [options] image_*.cbf" % (
  libtbx.env.dispatcher_name)

parser = OptionParser(
  usage=usage,
  phil=phil_scope,
  read_datablocks=True,
  read_datablocks_from_images=True)

params, options = parser.parse_args()
    

def merge_test_stats(all_stats):
    
    best_test_stats = {}
    for img, test_stats in all_stats.items():
        best_test_stats[img] = max([v for v in test_stats.values()], key=lambda t: t[1])
    return best_test_stats


def calc_stats(resol_dict, dfunc, dparams={}, func_name='N/A'):

    ks_stats = {}
    chi2_stats = {}
    fparams = dict(zip(['f0', 'f1', 'f2'], dparams.values()))
    for img, resol_list in resol_dict.items():
        if len(resol_list) < params.filter_grid.min_spots:
            continue
        lst_ = np.array(sorted(resol_list))
        ref_total = len(lst_)
        perc = [np.percentile(lst_, p) for p in np.linspace(0, 100, params.filter_grid.sample)]

        try:
            fit_vals = dfunc.fit(lst_, floc=0.0, **fparams)
            _, fit_scale = fit_vals
        except:
            try:
                _, _, fit_scale = fit_vals
            except:
                _, _, _, fit_scale = fit_vals

        hist_vals, hist_bins = np.histogram(lst_, bins=params.filter_grid.sample, density=False)
        calc_vals = np.array([dfunc.pdf(mean([x1, x2]), loc=0.0, scale=fit_scale, **dparams) * (x2 - x1) * ref_total for x1, x2 in
                              zip(hist_bins[:-1], hist_bins[1:])])

        cdf_ = partial(dfunc.cdf, loc=0.0, scale=fit_scale, **dparams)

        if img in params.filter_grid.profile_plots:
            plt.plot(hist_bins[1:], hist_vals, 'r--', linewidth=1, label='data')
            plt.plot(hist_bins[1:], calc_vals, 'g--', linewidth=1,label=' '.join([func_name, str(img)]))
            plt.legend()
            plt.show()

        sel_idx = [i for i, v in enumerate(zip(hist_vals, calc_vals)) if max(v) > 5]
        chi_sq, p_chisq = chisquare([hist_vals[i] for i in sel_idx],
                                    [calc_vals[i] for i in sel_idx])
        chi2_stats[img] = (chi_sq, p_chisq, len(resol_dict[img]))
        
        ks_D, ks_pval = stats.kstest(perc, cdf_)
        ks_stats[img] = (ks_D, ks_pval, len(resol_dict[img]))

    return {'ks': ks_stats, 'chi2': chi2_stats}


def output_json(results, filename):

    dct = {'images': [],
           'stat': [],
           'pval': [],
           'spots': []
           }
    for img, (D, pval, spots) in results.items():
        dct['images'].append(img)
        dct['stat'].append(D)
        dct['pval'].append(pval)
        dct['spots'].append(spots)
        
    with open('.'.join([filename, 'json']), 'w') as f:
        json.dump(dct, f)


def output_stats(test_stats, dfunc_name):

    lst_ = list(test_stats.items())
    ks_results_img = sorted(lst_, key=lambda v: v[0], reverse=False)[:]
    ks_results_spots = sorted(lst_, key=lambda v: v[1][-1], reverse=True)[:]
    
    from libtbx import table_utils
    for results, caption in [(ks_results_img, '%s results: sorted by image number' % dfunc_name),
                             (ks_results_spots,'%s results: sorted by number of spots' % dfunc_name)]:
        rows = [['Image', 'Stat.', 'P-value', '# spots'],]
        rows.extend([['%d' % img,
                      '%g' % D,
                      '%g' % pval,
                      '%d' % counts,
                      ] for img,(D, pval, counts) in results])
        print()
        print(caption)
        print(table_utils.format(rows, has_header=True,))
    
    return ks_results_img, ks_results_spots

    
def plot_stats(stats, images=None, title=''):
    
    stat_names = stats[stats.keys()[0]].keys()
    img_list = images if images else range(max(stats.keys()))
    if 'score' in params.filter_grid.plots:
        fig, ax = plt.subplots()
        for stat_idx, st in enumerate(stat_names):
            img_idx = []
            vals = []
            width = 0.8 / len(stat_names)
            for idx, i in enumerate(img_list):
                img_idx.append(idx)
                try:
                    val = stats[i][st][1]
                    vals.append(-1./ log10(val))
                except:
                    vals.append(0.)
            ax.bar(np.array(img_idx) + stat_idx*width, np.array(vals), label=st, width=width)
        plt.xticks(img_idx, img_list, rotation=90)
        yline= -1./ log10(params.filter_grid.threshold)
        plt.axhline(y=yline, color='r')
        ax.set(xlabel='Image', ylabel='Score', title=title)
        plt.legend()
        plt.show()

    if 'spots' in params.filter_grid.plots:
        img_idx = []
        vals = []
        fig, ax = plt.subplots()
        for idx, i in enumerate(img_list):
            img_idx.append(idx)
            val = stats[i][stat_names[0]][1]
            vals.append(val)
        ax.bar(np.array(img_idx), np.array(vals), label='Spots')
        plt.xticks(img_idx, img_list, rotation=90)
        ax.set(xlabel='Image', ylabel='# spots', title=title)
        plt.legend()
        plt.show()


def cross_ksstat(data_dict, images):
    
    ks_stats = {}
    for img1, img2 in combinations(data_dict.keys(), 2):
        lst1_ = data_dict[img1]
        lst2_ = data_dict[img2]
        D12, p_val12 = stats.ks_2samp(lst1_, lst2_)
        ks_stats[(img1, img2)] = (D12, p_val12)

    max_res_num = min(1000, len(data_dict))
    ks_results_D    = sorted(list(ks_stats.items()), key=lambda v: v[1][0], reverse=False)[:max_res_num]
    ks_results_pval = sorted(list(ks_stats.items()), key=lambda v: v[1][1], reverse=True)[:max_res_num]
        
    #print'_' * 80
    #print "Results correlations: best D"
    #pprint(ks_results_D)
    #print "Results correlations: best p-values"
    #pprint(ks_results_pval)
    #set_idx = set([v for v,_ in ks_results_pval]).intersection([v for v,_ in ks_results_D])
    #ks_results_total = [ (idx, st) for (idx, st) in ks_results_D if idx in set_idx] 
    #print "Results Correlations: overall "
    #pprint(ks_results_total)
    
    map_D = np.zeros([max(images), max(images)])
    map_pval = np.zeros([max(images), max(images)])
    for i, j in ks_stats:
        map_D[i][j], map_pval[i][j] = ks_stats[(i, j)]
    fig, ax = plt.subplots()
    im = ax.imshow(map_pval, interpolation='spline16', cmap=cm.afmhot)
    fig.colorbar(im, ax=ax)
    plt.show()
        
    return ks_results_D, ks_results_pval


if __name__ == '__main__':
  
    datablocks = flatten_datablocks(params.input.datablock)
  
    if len(datablocks) == 0:
        parser.print_help()
        exit()
  
    assert(len(datablocks) == 1)
  
    datablock = datablocks[0]
    imagesets = datablock.extract_imagesets()
  
    assert(len(imagesets) == 1)
  
    imageset = imagesets[0]
  
    #images = imageset.indices()
    detector = imageset.get_detector()
    beam = imageset.get_beam()
  
    # Configure the logging
    log.config(
      params.verbosity,
      info=params.output.log,
      debug=params.output.debug_log)

    resol_dict = {}
    reflections = flex.reflection_table.from_observations(
      datablock, params)
    
    for refl in reflections:
        x, y, z = refl['xyzobs.px.value']
        frame = int(ceil(z))
        resol = 1. / detector[0].get_resolution_at_pixel(beam.get_s0(), (x, y))**2
        try:
            resol_dict[frame].append(resol) 
        except KeyError:
            resol_dict[frame] = [resol,] 

    rayleigh_func = partial(calc_stats,
                            dfunc=stats.rayleigh)
    
    gengamma_shape = params.distribution.gengamma.shape
    gengamma_func = partial(calc_stats,
                            dfunc=stats.gengamma,
                            dparams=OrderedDict([('a', 1. / gengamma_shape),
                                                 ('c', gengamma_shape)]))
    
    chi2_low_scale = params.distribution.chi2_low.k
    chi2_low_func = partial(calc_stats,
                        dfunc=stats.chi2, dparams=OrderedDict([('df', chi2_low_scale),]))
    
    chi2_high_scale = params.distribution.chi2_high.k
    chi2_high_func = partial(calc_stats,
                        dfunc=stats.chi2, dparams=OrderedDict([('df', chi2_high_scale),]))
    
    expon_func = partial(calc_stats,
                         dfunc=stats.expon)

    #cross_ksstat(resol_dict, imageset.indices())
    
    distribution_dict = {'expon': expon_func,
                         'rayleigh': rayleigh_func,
                         'chi2_low': chi2_low_func,
                         'chi2_high': chi2_high_func,
                         'gengamma': gengamma_func
                         }
                      
    all_stats = {}
    sc = params.filter_grid.scoring
    thres_pval = lambda v: True if params.filter_grid.show_all else v[1] > params.filter_grid.threshold
    for func_name  in params.filter_grid.profiles:
        test_dict = dict((k, v) for k, v
                         in distribution_dict[func_name](resol_dict, func_name=func_name)[sc].items()
                         if thres_pval(v))
        output_stats(test_dict, func_name)
        output_json(test_dict, '_'.join([func_name, sc, 'stats']))
        for k, v in test_dict.items():
            try:
                all_stats[k].update({func_name: v})
            except:
                all_stats[k] = {func_name: v}

    merged_stats = merge_test_stats(all_stats)
    all_results = output_stats(merged_stats, 'Total')
    output_json(merged_stats, 'merged_results')

    for res, plot_title in zip(all_results,
                               ('Results sorted per image number',
                                'Results sorted per # of spots')):
        images = [img_ for (img_, _) in res]
        plot_stats(all_stats,
                   images=images,
                   title=plot_title)
