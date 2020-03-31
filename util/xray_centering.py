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


def get_neighbours(cell):
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
    # Step 1, find contiguous regions
    regions = []
    for cell in rc:
        surround = get_neighbours(cell)
        for assigned_region in regions:
            if surround.intersection(assigned_region):
                # at least one neighbour of this cell is
                # already assigned to another region
                assigned_region.add(cell)
                break
        else:
            regions.append({cell})

    # Step 2, merge neighbouring regions
    merged_regions = []
    for region in regions:
        for cell in region:
            surround = get_neighbours(cell)
            for seen_region in merged_regions:
                if surround.intersection(seen_region):
                    seen_region.update(region)
                    break
            else:
                merged_regions.append(region)

    # Note 1: The above code is fundamentally broken. Either it should merge
    # regions until no more regions can be merged, or it should apply a
    # space-filling algorithm from the start. As it stands this will not
    # correctly merge regions in eg. the case where there are 3 contiguous
    # regions with 1 not bordering 2, 2 bordering 3, and 3 bordering 1.
    # This will result in merged regions (1,3) and (2)

    # Note 2: The get_neighbours() function is also broken as it does not account
    # for borders on the 2D grid. Ie. the neighbour of x=0,y=4 is x=-1,y=4, which
    # is then resolved to the x=max,y=3 cell.

    # transform the sensible data structure back into something the next code
    # block understands
    regions = {list(sorted(v))[0]: list(sorted(v)) for v in merged_regions}

    # Step 3, find the dominant region

    length = 0
    sum1 = 0
    for k in regions:
        sum2 = 0
        if len(regions[k]) > length:
            (index, length) = (k, len(regions[k]))
            for cell in regions[k]:
                sum1 += g2i(cell)
                # Note 3a: sum1 is not re-set here, so we keep increasing this
                # number whenever we find larger regions.
        if len(regions[k]) == length:
            for cell in regions[k]:
                sum2 += g2i(cell)
            if sum2 > sum1:
                # Note 3b: we use sum1 here to tie-break between regions of
                # identical size, but sum1 can be the sum of multiple regions.
                sum1 = sum2
                (index, length) = (k, len(regions[k]))
    best_region = regions[index]
    return regions, best_region
