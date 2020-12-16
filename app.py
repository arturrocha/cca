import csv
import lib.database


def main():
    stations = lib.database.find_all("ntleg_2030")
    with open('results.csv', 'w+') as working_file:
        working_file.write('Station,Node,Type,AM,LT,SR,PM,_AM,_LT,_SR,_PM\n')
        for station in stations:
            if station["type"] == "Boarding":
                letter = 'b'
                node_search = 'a'
            elif station['type'] == 'Alighting':
                letter = 'a'
                node_search = 'b'

            node_filter = lib.database.find_filter("on_offs_nt", {letter: station["station_nb"]})
            all_zones = []
            for node in node_filter:
                zone_filter = lib.database.find_filter("zones", {"zone": node[node_search]})
                for zone in zone_filter:
                    del zone["_id"]
                    if zone not in all_zones:
                        all_zones.append(zone)
        
            vol = {}
            for zone in all_zones:
                for node in node_filter:
                    if zone["zone"] == node[node_search]:
                        float_vol = float(node['vol'])
                        try:
                            vol[node['period']][zone['zone_name']].append(float_vol)
                        except:
                            try:
                                vol[node['period']][zone['zone_name']] = []
                                vol[node['period']][zone['zone_name']].append(float_vol)
                            except:
                                vol[node['period']] = {}
                                vol[node['period']][zone['zone_name']] = []
                                vol[node['period']][zone['zone_name']].append(float_vol)
    
    
            for k, v in vol.items():
                if k == 'am':
                    am_math = 0
                    try:
                        am_math += sum(v['5_10min']) * 0.1
                    except:
                        am_math += 0                 
                    try:
                        am_math += sum(v['over10min']) * 0.5
                    except:
                        am_math += 0
                    am_math = str(round(am_math, 2))
                elif k == 'lt':
                    try:
                        lt_math = str(round(sum(v['over5min']) * 0.1, 2))
                    except:
                        lt_math = "0"
                elif k == 'sr':
                    try:
                        sr_math = str(round(sum(v['over5min']) * 0.1, 2))
                    except:
                        sr_math = "0"
                elif k == 'pm':
                    try:
                        pm_math = str(round(sum(v['over5min']) * 0.1, 2))
                    except:
                        pm_math = "0"
        

            this_row = ",".join([station["station_name"], 
                                station["station_nb"], 
                                station["type"], 
                                station["am"], 
                                station["lt"], 
                                station["sr"], 
                                station["pm"], 
                                am_math, 
                                lt_math, 
                                sr_math, 
                                pm_math, 
                                "\n"])
            working_file.write(this_row)
            return


if __name__ == '__main__':
    main()
