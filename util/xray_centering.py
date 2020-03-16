from __future__ import absolute_import, division, print_function


def main(
    data,
    numBoxesX=14,
    numBoxesY=11,
    snaked=True,
    boxSizeXPixels=1.25,
    boxSizeYPixels=1.25,
    topLeft=(396.2, 241.2),
    orientation="horizontal",
):
    results = {"centre_x": 0, "centre_y": 0, "status": "fail", "message": ""}
    numBoxesX = int(numBoxesX)
    numBoxesY = int(numBoxesY)
    results["numBoxesX"] = numBoxesX
    results["numBoxesY"] = numBoxesY
    results["boxSizeXPixels"] = boxSizeXPixels
    results["boxSizeYPixels"] = boxSizeYPixels
    results["topLeft"] = topLeft

    output = "numBoxesX/Y: " + str(numBoxesX) + ", " + str(numBoxesY) + "\n"
    output += (
        "boxSizeX/YPixels: " + str(boxSizeXPixels) + ", " + str(boxSizeYPixels) + "\n"
    )
    output += "topLeft: " + str(topLeft) + "\n"
    data = tuple(count for position, count in sorted(data))

    if orientation == "vertical":

        def transpose(x):
            return (x % numBoxesX) * numBoxesY + (x // numBoxesX)

        data = [data[transpose(x)] for x in range(len(data))]

    maximum_spots = max(data)
    best_image = data.index(maximum_spots) + 1
    if maximum_spots == 0:
        results["message"] = "No good images found"
        return results, output
        results["message"] = "The crystal is not diffracting"
        return results, output
    results["best_image"] = best_image
    results["reflections_in_best_image"] = maximum_spots
    output += "There are %d reflections in image no %d.\n" % (maximum_spots, best_image)

    if snaked:

        def unsnake(x):
            row = x // numBoxesX
            if row % 2 == 1:
                return x + numBoxesX - 2 * (x % numBoxesX) - 1
            else:
                return x

        data = [data[unsnake(x)] for x in range(len(data))]

    output += "grid: \n"
    for row in range(numBoxesY):
        output += "["
        output += ", ".join(
            "%3d" % data[row * numBoxesX + col]
            if data[row * numBoxesX + col] >= 0.5 * maximum_spots
            else " - "
            for col in range(numBoxesX)
        )
        output += "]\n"

    data = [x if x >= 0.5 * maximum_spots else 0 for x in data]

    rc = []
    for i, d in enumerate(data):
        if d:
            row = int(i / numBoxesX)
            col = i % numBoxesX
            rc.append((row, col))

    def gridtoindex(g):
        return data[g[0] * numBoxesX + g[1]]

    regions, best_region = findRegion(rc, gridtoindex)
    output += "regions: " + str(regions) + "\n"
    output += "best_region: " + str(best_region) + "\n"
    results["best_region"] = best_region
    sum_x, sum_y = 0, 0
    for key in best_region:
        sum_x = sum_x + (key[1] + 0.5)
        sum_y = sum_y + (key[0] + 0.5)
    output += (
        "sum_x, sum_y, len(best_region): "
        + str(sum_x)
        + " "
        + str(sum_y)
        + " "
        + str(len(best_region))
        + "\n"
    )
    results["sum_x"] = sum_x
    results["sum_y"] = sum_y
    centre_x = topLeft[0] + sum_x / len(best_region) * boxSizeXPixels
    centre_y = topLeft[1] + sum_y / len(best_region) * boxSizeYPixels
    centre_x_box = sum_x / len(best_region)
    centre_y_box = sum_y / len(best_region)
    output += "centre_x,centre_y=" + str(centre_x) + "," + str(centre_y)
    results["centre_x"] = centre_x
    results["centre_y"] = centre_y
    results["centre_x_box"] = centre_x_box
    results["centre_y_box"] = centre_y_box
    results["status"] = "ok"
    results["message"] = "ok"
    return results, output


def getSurround(cell):
    up = (cell[0] - 1, cell[1])
    upleft = (cell[0] - 1, cell[1] - 1)
    left = (cell[0], cell[1] - 1)
    downleft = (cell[0] + 1, cell[1] - 1)
    down = (cell[0] + 1, cell[1])
    downright = (cell[0] + 1, cell[1] + 1)
    right = (cell[0], cell[1] + 1)
    upright = (cell[0] - 1, cell[1] + 1)
    surround = {up, down, left, right, upleft, downleft, upright, downright}
    return surround


def findRegion(rc, g2i):
    regions = {}
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
        regions[cell] = [cell]
    for key in regions.keys():
        if key not in regions.keys():  # might have deleted it
            continue
        for cell in regions[key]:
            surround = getSurround(cell)
            for key2 in regions.keys():
                if key == key2:
                    continue
                if len(surround.intersection(regions[key2])) > 0:
                    regions[key].extend(regions[key2])
                    del regions[key2]
    length = 0
    sum1 = 0
    for k in regions:
        sum2 = 0
        if len(regions[k]) > length:
            (index, length) = (k, len(regions[k]))
            for cell in regions[k]:
                sum1 += g2i(cell)
        if len(regions[k]) == length:
            for cell in regions[k]:
                sum2 += g2i(cell)
            if sum2 > sum1:
                sum1 = sum2
                (index, length) = (k, len(regions[k]))
    best_region = regions[index]
    return regions, best_region
