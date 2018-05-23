from __future__ import absolute_import, division, print_function

import sys
import json

from pprint import pprint
from six.moves import cStringIO as StringIO

data = [(2, 73), (1, 0), (4, 119), (3, 187), (5, 2), (6, 0), (8, 0), (7, 0), (9, 0), (10, 0), (11, 0), (12, 0), (13, 0), (14, 0), (15, 0), (16, 0), (17, 0), (18, 0), (19, 0), (20, 0), (21, 0), (22, 0), (23, 144), (24, 449), (25, 539), (26, 418), (27, 141), (28, 2), (29, 0), (30, 100), (31, 402), (32, 592), (33, 538), (34, 394), (35, 221), (36, 0), (37, 0), (38, 0), (39, 0), (40, 0), (41, 0), (42, 0), (43, 0), (44, 0), (45, 0), (46, 0), (47, 0), (48, 0), (49, 151), (50, 352), (51, 440), (52, 506), (53, 515), (54, 229), (55, 13), (56, 0), (58, 2), (57, 0), (59, 27), (60, 229), (61, 415), (62, 481), (64, 389), (65, 90), (63, 387), (67, 0), (66, 1), (68, 0), (69, 0), (70, 0), (71, 0), (72, 0), (73, 0), (74, 0), (77, 436), (76, 441), (78, 385), (79, 289), (80, 96), (75, 107), (81, 74), (82, 18), (83, 0), (84, 0), (85, 0), (86, 0), (87, 0), (88, 25), (89, 26), (90, 41), (91, 179), (92, 376), (93, 382), (94, 295), (95, 28), (96, 0), (97, 0), (98, 0), (99, 0), (100, 0), (101, 14), (102, 115), (103, 217), (104, 159), (105, 52), (106, 29), (107, 44), (109, 0), (108, 9), (110, 0), (111, 0), (112, 0), (113, 0), (114, 0), (115, 0), (116, 0), (117, 0), (118, 11), (119, 57), (120, 24), (121, 22), (122, 16), (123, 32), (124, 41), (125, 11), (126, 0), (127, 26), (128, 48), (129, 27), (130, 24), (131, 16), (132, 8), (134, 0), (133, 2), (135, 0), (136, 0), (137, 0), (138, 0), (139, 0), (140, 0), (141, 0), (143, 0), (142, 0), (144, 0), (145, 0), (146, 0), (147, 0), (148, 0), (149, 0), (150, 0), (151, 0), (152, 10), (153, 28), (154, 48)]

def main(data, numBoxesX=14, numBoxesY=11, snaked=True, boxSizeXPixels=1.25, boxSizeYPixels=1.25, topLeft=(396.2, 241.2)):
  results = { "centre_x": 0, "centre_y": 0, "status": "fail", "message": ""}
  numBoxesX = int(numBoxesX)
  numBoxesY = int(numBoxesY)
  results['numBoxesX'] = numBoxesX
  results['numBoxesY'] = numBoxesY
  results['boxSizeXPixels'] = boxSizeXPixels
  results['boxSizeYPixels'] = boxSizeYPixels
  results['topLeft'] = topLeft
  f = StringIO()

  if True:
    ns, vs = zip(*filter(lambda x: bool(x[1]), data))
    results['ns'] = ns
    results['vs'] = vs
    f.write("ns: "+str(ns)+"\n")
    f.write("vs: "+str(vs)+"\n")
    f.write("numBoxesX/Y: "+str(numBoxesX)+", "+str(numBoxesY)+"\n")
    f.write("boxSizeX/YPixels: "+str(boxSizeXPixels)+", "+str(boxSizeYPixels)+"\n")
    f.write("topLeft: "+str(topLeft)+"\n")
#    data = dict(data)
    data = zip(*sorted(data))[1]
#    best_image, maximum_spots = max(data.items(), key=lambda d: d[1])
    maximum_spots = max(data)
    best_image = data.index(maximum_spots) + 1
    if maximum_spots == 0:
      results["message"] = "No good images found"
      return results
      results["message"] = "The crystal is not diffracting"
      return results
    results['best_image'] = best_image
    results['reflections_in_best_image'] = maximum_spots
    f.write("There are %d reflections in image no %d.\n" % (maximum_spots, best_image))
    selected_images = [ n + 1 for n, s in enumerate(data) if s >= 0.5 * maximum_spots ]
#  ist(filter(lambda n, d: s >= 0.5*maximum_spots, enumerate(data)))
    f.write("selected_images: "+str(selected_images)+"\n")
    results['selected_images'] = selected_images

    if snaked:
      def unsnake(x):
#        row = (x - 1) // numBoxesX
        row = x // numBoxesX
        if row % 2 == 1:
#         return x + numBoxesX - 1 - 2 * ((x-1) % numBoxesX)
          return x + numBoxesX - 2 * (x % numBoxesX) - 1
        else:
          return x
      data = [ data[unsnake(x)] for x in range(len(data)) ]
#      data = { unsnake(x): data[x] for x in data }
#      data = unsnake_data

    f.write("grid: \n")
    for row in range(numBoxesY):
      f.write("[")
      f.write(", ".join(
          "%3d" % data[row * numBoxesX + col]
          if data[row * numBoxesX + col] >= 0.5 * maximum_spots
          else ' - '
          for col in range(numBoxesX)
      ))
      f.write("]\n")

    data = [ x if x >= 0.5*maximum_spots else 0 for x in data ]

    rc = []
    for i, d in enumerate(data):
      if d:
        row=int(i/numBoxesX)
        col=(i%numBoxesX)
#      row=int((i-1)/numBoxesX)
#      col=((i-1)%numBoxesX)
        rc.append((row, col))

    def gridtoindex(g):
 #     return data[g[0] * numBoxesX + g[1]]
      return data[g[0] * numBoxesX + g[1] + 1]

    regions, best_region = findRegion(rc, gridtoindex)
    f.write("regions: "+str(regions)+"\n")
    f.write("best_region: "+str(best_region)+"\n")
    results['best_region'] = best_region
    sum_x, sum_y = 0,0
    for key in best_region:
      sum_x=sum_x+(key[1]+.5)
      sum_y=sum_y+(key[0]+.5)
    f.write("sum_x, sum_y, len(best_region): "+str(sum_x)+" "+str(sum_y)+" "+str(len(best_region))+"\n")
    results['sum_x'] = sum_x
    results['sum_y'] = sum_y
    centre_x=topLeft[0]+sum_x/len(best_region)*boxSizeXPixels
    centre_y=topLeft[1]+sum_y/len(best_region)*boxSizeYPixels
    centre_x_box = sum_x/len(best_region)
    centre_y_box = sum_y/len(best_region)
    f.write("centre_x,centre_y="+str(centre_x)+","+str(centre_y))
    results['centre_x'] = centre_x
    results['centre_y'] = centre_y
    results['centre_x_box'] = centre_x_box
    results['centre_y_box'] = centre_y_box
    results['status'] = 'ok'
    results['message'] = 'ok'
  return results, f.getvalue()

def getSurround(cell):
  up = (cell[0]-1,cell[1])
  upleft = (cell[0]-1,cell[1]-1)
  left = (cell[0],cell[1]-1)
  downleft = (cell[0]+1,cell[1]-1)
  down = (cell[0]+1,cell[1])
  downright = (cell[0]+1,cell[1]+1)
  right = (cell[0],cell[1]+1)
  upright = (cell[0]-1,cell[1]+1)
  surround = set([up,down,left,right,upleft,downleft,upright,downright])
  return surround

def findRegion(rc, g2i):
  regions={}
  for cell in rc:
          found = False
          for region in regions.values():
                  if cell in region:
                          found = True
                          break
          if found:
                  continue
          surround = getSurround(cell)
          added = False
          for key in regions.keys():
                  if len(surround.intersection(regions[key])) > 0:
                          regions[key].append(cell)
                          added = True
                          break
          if added:
                  continue
          regions[cell]=[cell]
  for key in regions.keys():
          if key not in regions.keys(): #might have deleted it
                  continue
          for cell in regions[key]:
                  surround = getSurround(cell)
                  for key2 in regions.keys():
                          if key == key2:
                                  continue
                          if len(surround.intersection(regions[key2])) > 0:
                                  regions[key].extend(regions[key2])
                                  del(regions[key2])
  length=0
  sum1=0
  for k in regions:
          sum2=0
          if len(regions[k]) > length:
                  (index, length)=(k, len(regions[k]))
                  for cell in regions[k]:
                          sum1 += g2i(cell)
          if len(regions[k]) == length:
                  for cell in regions[k]:
                          sum2 += g2i(cell)
                  if sum2 > sum1:
                          sum1 = sum2
                          (index, length)=(k, len(regions[k]))
  best_region=regions[index]
  return regions, best_region

if __name__ == "__main__":
  results, stdout = main(data)
  print(stdout)
  print("---")
  print(json.dumps(results, sort_keys=True))
#       with open("Dials5AResults.json","w") as f2:
#               results = main(spot_file, numBoxesX, numBoxesY, snaked, boxSizeXPixels, boxSizeYPixels, topLeft)
#               json.dump(results, f2, sort_keys=True)
