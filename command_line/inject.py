import stomp

conn = stomp.Connection([('ws154.diamond.ac.uk', 61613)])
conn.start()
conn.connect('admin', 'password', wait=True)
headers = {}
conn.send('/queue/run_xia2', '{"parameters": [ "image=/dls/mx-scratch/mgerstel/zenodo-NiDPPE/data/j3_01_00001.cbf", "small_molecule=true" ]}', headers=headers)
