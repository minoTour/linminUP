def process_pre_local_basecalling_fast5(
    oper,
    db,
    connection_pool,
    args,
    ref_fasta_hash,
    dbcheckhash,
    filepath,
    hdf,
    dbname,
    cursor,
    bwaclassrunner,
    ):

    try: checksum = hashlib.md5(open(filepath, 'rb').read()).hexdigest()
    except: 
        err_string = "process_fast5(): error checksum ", filepath
        print >> sys.stderr, err_string
        sys.exit()

    # print checksum, type(checksum)
    # ## find the right basecall_2D location, get configuaration genral data, and define the basename.

    read_type = check_read_type(args, filepath, hdf)
    if args.verbose == "high": 
        print "@@ "+ ('='*40)
        print "@@ filepath:", filepath
        print "@@ read_type:", read_type
    #print "FILETYPE is", read_type


#--------------------------------------------------------------------------------
# Set Bascall Dir stuff ...

    '''
# Fast5 File Crib ....

EBOLA # Type 1
GROUP "/" {
   GROUP "Analyses" {
      GROUP "Basecall_2D_000" {
         GROUP "BaseCalled_2D" {
         GROUP "BaseCalled_complement" {
         GROUP "BaseCalled_template" {
        ...

AGBT: # Type 2
GROUP "/" {
   GROUP "Analyses" {
      GROUP "Basecall_1D_000" {
         GROUP "BaseCalled_complement" {
         GROUP "BaseCalled_template" {
        ...

    '''


    # print "REF", ref_fasta_hash
    basecalltype = getBasecalltype(args, read_type)
    basecalldir = ''
    basecalldir2 = ''
    basecalldirconfig = ''
    basecallindexpos='' #ML

    for x in range(0, 9):
        string = '/Analyses/%s_00%s/Configuration/general' \
            % (basecalltype, x)

        if string in hdf:
            basecallindexpos=x #ml
            basecalldir = '/Analyses/%s_00%s/' % (basecalltype, x)

            if read_type==1:
                basecalldir1 = '/Analyses/Basecall_2D_00%s/' % \
                                (basecallindexpos)
            else:
                basecalldir1 = '/Analyses/Basecall_1D_00%s/' % \
                                (basecallindexpos)

            basecalldir2 = '/Analyses/Basecall_2D_00%s/' % \
                                (basecallindexpos)
            basecalldirconfig = string
            break

    if args.verbose == "high":
            print string
            print basecalldir
            print basecalldir1
            print basecalldir2
            sys.stdout.flush()
            #sys.exit()

    configdata = hdf[basecalldirconfig]
    basename = configdata.attrs['basename']  # = PLSP57501_17062014lambda_3216_1_ch101_file10_strand


#--------------------------------------------------------------------------------
# Process Traking ID stuff ... --> Tracking ID Hash
    # # get all the tracking_id data, make primary entry for basename, and get basenameid

    tracking_id_fields = [
        'basename',
        'asic_id',
        'asic_id_17',
        'asic_id_eeprom',
        'asic_temp',
        'device_id',
        'exp_script_purpose',
        'exp_script_name',
        'exp_start_time',
        'flow_cell_id',
        'heatsink_temp',
        'hostname',
        'run_id',
        'version_name',
        ]
    tracking_id_hash = make_hdf5_object_attr_hash(args,
            hdf['/UniqueGlobalKey/tracking_id'], tracking_id_fields)
    tracking_id_hash.update({'basename': basename,
                            'file_path': filepath, 'md5sum': checksum})
    hdf5object = hdf['/UniqueGlobalKey/channel_id']

        # print "Got event location"

    for x in ('channel_number', 'digitisation', 'offset',
              'sampling_rate'):
        if x in hdf5object.attrs.keys():
            value = str(hdf5object.attrs[x])

            # print x, value

            tracking_id_hash.update({x: value})

    # range is a specifal case:
    # for x in ('range'):
    #    if (x in hdf5object.attrs.keys() ):
    #        value=str(hdf5object.attrs[x])
    #        print x, value
    #        tracking_id_hash.update({'range_val ':value})

    passcheck = 0
    if '/pass/' in filepath:
        passcheck = 1
    if '\\pass\\' in filepath:
        passcheck = 1
    tracking_id_hash.update({'pass': passcheck})
    basenameid = mysql_load_from_hashes(args, db, cursor, 'tracking_id',
            tracking_id_hash)

#--------------------------------------------------------------------------------
# Process Config General stuff ... --> General Hash

    # # get all the data from Configuration/general, then add Event Detection mux pore number

    general_fields = [
        'basename',
        'local_folder',
        'workflow_script',
        'workflow_name',
        'read_id',
        'use_local',
        'tag',
        'model_path',
        'complement_model',
        'max_events',
        'input',
        'min_events',
        'config',
        'template_model',
        'channel',
        'metrichor_version',
        'metrichor_time_stamp',
        ]
    general_hash = make_hdf5_object_attr_hash(args, configdata,
            general_fields)
    general_hash.update({'basename_id': basenameid})
    general_hash.update({'read_type': getBasecalltype(args, read_type)})
    if (len(basecalldir)>0): #ML
        metrichor_info=hdf[basecalldir] #ML
        try: general_hash.update({'metrichor_version':metrichor_info.attrs['chimaera version'], 'metrichor_time_stamp':metrichor_info.attrs['time_stamp']}) #ML
        except: general_hash.update({'metrichor_version':metrichor_info.attrs['version'], 'metrichor_time_stamp':metrichor_info.attrs['time_stamp']}) #ML
    else: #ML
        general_hash.update({'metrichor_version':'N/A', 'metrichor_time_stamp':''}) #ML

#--------------------------------------------------------------------------------
# Process Event Detection Stuff ... --> GENERAL HASH

    # # get event detection for the read; define mux pore nuber


    eventdectionreadstring = \
        '/Analyses/EventDetection_000/Reads/Read_%s' \
        % general_hash['read_id']
    if eventdectionreadstring in hdf:
        hdf5object = hdf[eventdectionreadstring]

        # print "Got event location"

        for x in (
            'start_mux',
            'end_mux',
            'abasic_event_index',
            'abasic_found',
            'abasic_peak_height',
            'duration',
            'hairpin_event_index',
            'hairpin_found',
            'hairpin_peak_height',
            'hairpin_polyt_level',
            'median_before',
            'read_number',
            'scaling_used',
            'start_time',
            ):
            if x in hdf5object.attrs.keys():
                value = str(hdf5object.attrs[x])

                # print x, value

                general_hash.update({x: value})

        # Specific to catch read_id as different class:

        for x in 'read_id':
            if x in hdf5object.attrs.keys():
                value = str(hdf5object.attrs[x])

                # print 'read_name', value

                general_hash.update({'read_name': value})

        # Add pass flag to general_hash

        general_hash.update({'pass': passcheck})

        exp_start_time = int(tracking_id_hash['exp_start_time' ])
        exp_start_time_f = frmt(exp_start_time)

        if args.verbose == "high": 
                print "@@ exp start_time: ", exp_start_time_f
        general_hash.update({'exp_start_time': exp_start_time})

        sampling_rate = \
           float(tracking_id_hash['sampling_rate']) * 60.

        start_time = \
           float(hdf5object.attrs['start_time']) / sampling_rate

        g_start_time = exp_start_time + int(start_time)*60

        # ------------------------------------------
        # 0.64b ...
        # Use End time == Start time of final event ...

        end_time = \
           float(hdf[ eventdectionreadstring + "/Events"][-1][-2]) \
                                / sampling_rate 
        g_end_time = exp_start_time + int(end_time)*60

        if args.verbose == "high": 
                print "@@ start / end times A Line 296: ", start_time, end_time
                print "@@ g_start / g_end times g_A Line 296: ", frmt(g_start_time), frmt(g_end_time)
                sys.stdout.flush()
                #sys,exit()

        template_start = start_time 
        template_end = end_time 


        
        # Scale global times to minutes .....
        g_start_time = int(g_start_time / 60)
        g_end_time = int(g_end_time / 60)
        g_template_start = g_start_time 
        g_template_end = g_end_time 


        # ------------------------------------------
        
        general_hash.update({'1minwin': int(end_time/ 1.)})  
        general_hash.update({'5minwin': int(end_time/ 5.)})  
        general_hash.update({'10minwin': int(end_time/ 10.)})  
        general_hash.update({'15minwin': int(end_time/ 15.)})
        general_hash.update({'s1minwin': int(start_time/ 1.)})  
        general_hash.update({'s5minwin': int(start_time/ 5.)})  
        general_hash.update({'s10minwin': int(start_time/ 10.)})  
        general_hash.update({'s15minwin': int(start_time/ 15.)})
        general_hash.update({'g_1minwin': int(g_end_time/ 1.)})  
        general_hash.update({'g_5minwin': int(g_end_time/ 5.)})  
        general_hash.update({'g_10minwin': int(g_end_time/ 10.)})  
        general_hash.update({'g_15minwin': int(g_end_time/ 15.)})
        general_hash.update({'g_s1minwin': int(g_start_time/ 1.)})  
        general_hash.update({'g_s5minwin': int(g_start_time/ 5.)})  
        general_hash.update({'g_s10minwin': int(g_start_time/ 10.)})  
        general_hash.update({'g_s15minwin': int(g_start_time/ 15.)})

        # if ('start_mux' in hdf5object.attrs.keys() ):
        #    start_mux=str(hdf5object.attrs['start_mux'])
            # print "start_mux", start_mux
        #    general_hash.update({'start_mux':start_mux})
        # if ('end_mux' in hdf5object.attrs.keys() ):
        #    stop_mux=str(hdf5object.attrs['end_mux'])
            # print "stop_mux", stop_mux
        #    general_hash.update({'end_mux':stop_mux})

    # ## load general_hash into mysql

    mysql_load_from_hashes(args, db, cursor, 'config_general', general_hash)


#--------------------------------------------------------------------------------
# Process Basecll Summary Stuff ... Basecall Summary hash

    # # get all the basecall summary split hairpin data

    basecall_summary_fields = [
        'abasic_dur',
        'abasic_index',
        'abasic_peak',
        'duration_comp',
        'duration_temp',
        'end_index_comp',
        'end_index_temp',
        'hairpin_abasics',
        'hairpin_dur',
        'hairpin_events',
        'hairpin_peak',
        'median_level_comp',
        'median_level_temp',
        'median_sd_comp',
        'median_sd_temp',
        'num_comp',
        'num_events',
        'num_temp',
        'pt_level',
        'range_comp',
        'range_temp',
        'split_index',
        'start_index_comp',
        'start_index_temp',
        ]


    if read_type == 1:
        basecall_summary_hash = make_hdf5_object_attr_hash(args,
                hdf[basecalldir + 'Summary/split_hairpin'],
                basecall_summary_fields)

    if read_type == 2:
        basecall_summary_hash=make_hdf5_object_attr_hash(args, 
                hdf['/Analyses/Hairpin_Split_00'+
                        str(basecallindexpos)+'/Summary/split_hairpin'],
                basecall_summary_fields)

    if read_type == 3: # MS NB Cludge to get it to run ....
        basecall_summary_hash = make_hdf5_object_attr_hash(args,
            hdf[basecalldir], # MS ...  + 'Summary/split_hairpin'],
            basecall_summary_fields)



    #print '/Analyses/Hairpin_Split_00'+str(basecallindexpos)+'/Summary/split_hairpin'
    #print basecall_summary_hash
    # # adding info about other the basecalling itself


    if basecalldir1 + 'Summary/basecall_1d_template' in hdf:
        hdf5object = hdf[basecalldir1 + 'Summary/basecall_1d_template']

        # print "Got event location"

        for x in (
            'drift',
            'mean_qscore',
            'num_skips',
            'num_stays',
            'scale',
            'scale_sd',
            'sequence_length',
            'shift',
            'strand_score',
            'var',
            'var_sd',
            ):
            if x in hdf5object.attrs.keys():
                value = str(hdf5object.attrs[x])

                # print x, value

                basecall_summary_hash.update({x + 'T': value})

    if basecalldir1 + 'Summary/basecall_1d_complement' in hdf:
        hdf5object = hdf[basecalldir1 + 'Summary/basecall_1d_complement']

        # print "Got event location"

        for x in (
            'drift',
            'mean_qscore',
            'num_skips',
            'num_stays',
            'scale',
            'scale_sd',
            'sequence_length',
            'shift',
            'strand_score',
            'var',
            'var_sd',
            ):
            if x in hdf5object.attrs.keys():
                value = str(hdf5object.attrs[x])

                # print x, value

                basecall_summary_hash.update({x + 'C': value})

    # # adding info about other the basecalling itself

    if basecalldir2 + 'Summary/basecall_2d' in hdf:
        hdf5object = hdf[basecalldir2 + 'Summary/basecall_2d']

        # print "Got event location"

        for x in ('mean_qscore', 'sequence_length'):
            if x in hdf5object.attrs.keys():
                value = str(hdf5object.attrs[x])

                # print x, value

                basecall_summary_hash.update({x + '2': value})


    '''
    if args.verbose == "high":
            print "="*60
            print "WE ARE HERE"
            for k,v in sorted(basecall_summary_hash.items()): print k,v
            print basecalldir
            print basecalldir1
            print basecalldir2
            sys.stdout.flush()
            sys.exit()
    '''

    # # Adding key indexes and time stamps
    if args.verbose == "high": 
        print "@@ start / end times B Line 429: ", start_time, end_time
        print "@@ g_start / g_end g_times B Line 429: ", \
                                frmt(g_start_time), frmt(g_end_time)
        sys.stdout.flush()

    basecall_summary_hash.update({'basename_id': basenameid})
    basecall_summary_hash.update({'pass': passcheck})
    basecall_summary_hash.update({'exp_start_time': tracking_id_hash['exp_start_time' ]})

    fields =    [ '1minwin', '5minwin', '10minwin', '15minwin'
                , 's1minwin', 's5minwin', 's10minwin', 's15minwin'
                , 'g_1minwin', 'g_5minwin', 'g_10minwin', 'g_15minwin'
                , 'g_s1minwin', 'g_s5minwin', 'g_s10minwin', 'g_s15minwin'
                ]

    for x in fields:
        basecall_summary_hash.update({x: general_hash[x]})

    # print basecall_summary_hash

    # # load basecall summary hash into mysql

    mysql_load_from_hashes(args, db, cursor, 'basecall_summary',
                           basecall_summary_hash)
#--------------------------------------------------------------------------------
# Process Barcode Stuff ... --> Bacode hash

    # # see if there is any barcoding info to addd

    barcode_hash = dict()
    for x in range(0, 9):
        string = '/Analyses/Barcoding_00%s/Summary/barcoding' % x

        # print string

        if string in hdf:

            # print "barcode", string

            barcode_hash = make_hdf5_object_attr_hash(args,
                    hdf[string], (
                'pos0_start',
                'score',
                'design',
                'pos1_end',
                'pos0_end',
                'pos1_start',
                'variant',
                'barcode_arrangement',
                ))
            barcode_hash.update({'basename_id': basenameid})
            mysql_load_from_hashes(args,  db, cursor, 'barcode_assignment',
             barcode_hash)

            # print barcode_hash
            # for bk in barcode_hash.keys():
            #    print bk, barcode_hash[bk], type(barcode_hash[bk])

            break

#--------------------------------------------------------------------------------
# Process Model Stuff  ...

    # ------------ Do model details -------------------

    if args.telem is True:
        if dbname not in dbcheckhash['modelcheck']:
            dbcheckhash['modelcheck'][dbname] = dict()

        log_string = basecalldir + 'Log'
        if log_string in hdf:
            log_data = str(hdf[log_string][()])

            # print type(log), log

            lines = log_data.split('\n')
            template_model = None
            complement_model = None
            for l in lines:
                t = re.match('.*Selected model: "(.*template.*)".', l)
                if t:
                    template_model = t.group(1)
                c = re.match('.*Selected model: "(.*complement.*)".', l)
                if c:
                    complement_model = c.group(1)

            if template_model is not None:
                sql = \
                    "INSERT INTO %s (basename_id,template_model,complement_model) VALUES ('%s','%s',NULL)" \
                    % ('model_list', basenameid, template_model)
                if template_model not in dbcheckhash['modelcheck'
                        ][dbname]:
                    location = basecalldir + 'BaseCalled_template/Model'
                    if location in hdf:
                        upload_model_data('model_data', template_model,
                                location, hdf, cursor, db)
                        dbcheckhash['modelcheck'
                                    ][dbname][template_model] = 1

                if complement_model is not None:
                    sql = \
                        "INSERT INTO %s (basename_id,template_model,complement_model) VALUES ('%s','%s','%s')" \
                        % ('model_list', basenameid, template_model,
                           complement_model)
                    if complement_model not in dbcheckhash['modelcheck'
                            ][dbname]:
                        location = basecalldir \
                            + 'BaseCalled_complement/Model'
                        if location in hdf:
                            upload_model_data('model_data',
                                    complement_model, location, hdf,
                                    cursor, db)
                            dbcheckhash['modelcheck'
                                    ][dbname][complement_model] = 1
                if args.verbose == "high": print sql; debug()

                cursor.execute(sql)
                db.commit()

    # ---------------------------------------------------------------------------

    if read_type == 1:
        readtypes =  \
        { 'basecalled_template'  : basecalldir + 'BaseCalled_template/'
        , 'basecalled_complement': basecalldir + 'BaseCalled_complement/'
        , 'basecalled_2d'        : basecalldir + 'BaseCalled_2D/'
        }

    if read_type == 3:
        readtypes =  \
        { 'basecalled_template'  : basecalldir + 'BaseCalled_template/'
        , 'basecalled_complement': basecalldir + 'BaseCalled_complement/'
        , 'basecalled_1d'        : basecalldir + 'BaseCalled_1D/' # MS ??
        }

    if read_type == 2:
        readtypes =  \
        { 'basecalled_template'   : basecalldir1 + 'BaseCalled_template/'
        , 'basecalled_complement' : basecalldir1 +'BaseCalled_complement/'
        , 'basecalled_2d'         : basecalldir2 + 'BaseCalled_2D/'
        }


    '''
    # MS -- old school way ...
    if read_type == 2:
        readtypes = \
        { 'basecalled_template' : \
                '/Analyses/Basecall_1D_00%s/BaseCalled_template/'\
                        %(basecallindexpos)
        , 'basecalled_complement' : \
                '/Analyses/Basecall_1D_00%s/BaseCalled_complement/' \
                        %(basecallindexpos)
        , 'basecalled_2d' : \
                '/Analyses/Basecall_2D_00%s/BaseCalled_2D/'\
                        %(basecallindexpos)
        }
    '''





#--------------------------------------------------------------------------------
##@@
# Process Fasta basecall data ....

    if args.verbose == "high": debug()
    fastqhash = dict()

    # tel_sql_list=list()

    tel_data_hash = dict()
    template_start = 0
    template_end = 0
    g_template_start = 0
    g_template_end = 0


    readtypes = { 'basecalled_template'  : 
                    basecalldir + 'BaseCalled_template/' }

    for (readtype, location) in readtypes.iteritems():
        if location in hdf:
            fastq = hdf[location + 'Fastq'][()]
            try:
                rec = SeqIO.read(StringIO(fastq), 'fastq')
            except Exception, err:
                err_string = \
                    '%s:\tError reading fastq oject from base: %s type: %s error: %s' \
                    % (time.strftime('%Y-%m-%d %H:%M:%S'), basename,
                       readtype, err)
                print >> sys.stderr, err_string
                with open(dbcheckhash['logfile'][dbname], 'a') as \
                    logfilehandle:
                    logfilehandle.write(err_string + os.linesep)
                    logfilehandle.close()
                continue

            sequence = str(rec.seq)
            seqlen = len(sequence)
            rec.id = basename + '.' + readtype

            qual = chr_convert_array(db,
                    rec.letter_annotations['phred_quality'])
            fastqhash[rec.id] = \
                {'quals': rec.letter_annotations['phred_quality'],
                 'seq': sequence}

            if location + 'Alignment' in hdf:  # so its 2D

                # print "we're looking at a 2D read",template_start,"\n\n"

                

                mysql_load_from_hashes(args, db, cursor, readtype, {
                    'basename_id': basenameid,
                    'seqid': rec.id,
                    'sequence': sequence,
                    'qual': qual,
                    'start_time': template_start,
                    'seqlen': seqlen,
                    'exp_start_time': tracking_id_hash['exp_start_time' ],
                    '1minwin': int(template_end / 1),
                    '5minwin': int(template_end / 5),
                    '10minwin': int(template_end / 10),
                    '15minwin': int(template_end / 15),
                    's1minwin': int(template_start / 60),
                    's5minwin': int(template_start / 5),
                    's10minwin': int(template_start / 10),
                    's15minwin': int(template_start / 15),
                    'g_1minwin': int(g_template_end / 1),
                    'g_5minwin': int(g_template_end / 5),
                    'g_10minwin': int(g_template_end / 10),
                    'g_15minwin': int(g_template_end / 15),
                    'g_s1minwin': int(g_template_start / 60),
                    'g_s5minwin': int(g_template_start / 5),
                    'g_s10minwin': int(g_template_start / 10),
                    'g_s15minwin': int(g_template_start / 15),
                    'pass': passcheck,
                    })
                if args.telem is True:
                    alignment = hdf[location + 'Alignment'][()]

                    # print "ALIGNMENT", type(alignment)

                    channel = general_hash['channel'][-1]
                    tel_data_hash[readtype] = [basenameid, channel,
                            alignment]

                    # upload_2dalignment_data(basenameid,channel,alignment,db)
                    # tel_sql_list.append(t_sql)

            complement_and_template_fields = [
                'basename',
                'seqid',
                'duration',
                'start_time',
                'scale',
                'shift',
                'gross_shift',
                'drift',
                'scale_sd',
                'var_sd',
                'var',
                'sequence',
                'qual',
                ]
        
            #if location + 'Events' in hdf and location + 'Model' in hdf:  
            if location + 'Events' in hdf and location + 'Fastq' in hdf:  
            # so its either template or complement
                events_hash = make_hdf5_object_attr_hash(args,
                        hdf[location + 'Events'],
                        complement_and_template_fields)
                if location + 'Model' in hdf:  
                    model_hash = make_hdf5_object_attr_hash(args,
                        hdf[location + 'Model'],
                        complement_and_template_fields)
                    events_hash.update(model_hash)

                # #Logging the start time of a template read to pass to the 2d read in order to speed up mysql processing

                if readtype == 'basecalled_template':

                    # 0.64a
                    ''' 
                    ???
                    start_time = float(events_hash['start_time']) 
                                 # /  sampling_rate
                    '''

                    events_table = hdf[ location + "/Events"]
                    start_time = float(events_table[0][1]) / 60.
                    g_start_time = exp_start_time + int(start_time)*60


                    # 0.64b
                    end_time = float(events_table[-1][1]) / 60.
                    g_end_time = exp_start_time + int(end_time)*60


                    template_start = start_time
                    template_end = end_time
                    g_template_start = g_start_time
                    g_template_end = g_end_time


        
                if args.verbose == "high": 
                        print "@@ location: ", location 
                
                        print "@@ template start / end times: ",  \
                                        template_start, template_end
                
                        print "@@ start / end times C Line 781",  \
                                        start_time, end_time
                
                        print "@@ g_template start / end times: ",  \
                            frmt(g_template_start), frmt(g_template_end)
                
                        print "@@ g_start / g_end times C Line 784", \
                            frmt(g_start_time), frmt(g_end_time)
                
                        print "@@ "+ ("-"*40)
                
                        sys.stdout.flush()
                #sys.exit()

                # Scale global times to minutes .....
                g_start_time = int(g_start_time / 60)
                g_end_time = int(g_end_time / 60)
                g_template_start = int(g_template_start / 60)
                g_template_end = int(g_template_end / 60)


                events_hash.update({
                    'basename_id': basenameid,
                    'seqid': rec.id,
                    'sequence': sequence,
                    'qual': qual,
                    'seqlen': seqlen,
                    '1minwin': end_time / 1,
                    '5minwin': end_time / 5,
                    '10minwin': end_time / 10,
                    '15minwin': end_time / 15,
                    's1minwin': start_time / 1,
                    's5minwin': start_time / 5,
                    's10minwin': start_time / 10,
                    's15minwin': start_time / 15,
                    'g_1minwin': g_end_time / 1,
                    'g_5minwin': g_end_time / 5,
                    'g_10minwin': g_end_time / 10,
                    'g_15minwin': g_end_time / 15,
                    'g_s1minwin': g_start_time / 1,
                    'g_s5minwin': g_start_time / 5,
                    'g_s10minwin': g_start_time / 10,
                    'g_s15minwin': g_start_time / 15,
                    })
                        

                events_hash.update({'exp_start_time': tracking_id_hash['exp_start_time'
                                   ], 'pass': passcheck})
                mysql_load_from_hashes(args, db, cursor, readtype,
                        events_hash)

                # -------- This inserts telemetry data. It is optional under the flags above.
                # -------- Modified to calculate some means and averages
        # ------- so we are going to do this everytime
                # if (args.telem is True):
                    # print "start telem",  (time.time())-starttime
                    # ## Do Events

                events = hdf[location + 'Events'][()]
                tablechannel = readtype + '_' + general_hash['channel'
                        ][-1]
                tel_data_hash[readtype] = [basenameid, tablechannel,
                        events]

                # # We want to calculate the mean current for a read here... how do we do that?
                # eventcounter=0
                # totalcurrent=0
                # meanlist=list()
                # for event in events:
                #    eventcounter+=1
                #    totalcurrent=totalcurrent + event[0]
                #    meanlist.append(event[0])
                # print numpy.median(numpy.array(meanlist))
                # print basenameid, basename,readtype,eventcounter,totalcurrent/eventcounter

    # ---------------------------------------------------------------------------

    # ## Then at this point we just need to go on and do the alignment...
    if dbname in ref_fasta_hash:  # so we're doing an alignment
        if fastqhash:  # sanity check for the quality scores in the hdf5 file. this will not exist if it's malformed.
            if args.last_align is True:
                if args.verbose == "high":
                    print 'LAST aligning....'
                init_last_threads(oper, args, connection_pool[dbname] \
                                , fastqhash, basename, basenameid \
                                , dbname, dbcheckhash, ref_fasta_hash)
                if args.verbose == "high":
                    print '... finished last aligning.'
            if args.bwa_align is True:
                if args.verbose == "high":
                    print 'BWA aligning....'
                bwaclassrunner.setjob(args,
                   ref_fasta_hash,
                   connection_pool[dbname],
                   fastqhash,
                   basename,
                   basenameid,
                   dbname,
                   )
                if args.verbose == "high":
                    print '... finished BWA aligning.'

            # for seqid in fastqhash.keys():    # use this for debugging instead of lines above that use threading
            #    #if ("template" in seqid):
            #    do_last_align(seqid, fastqhash[seqid], basename, basenameid, dbname, db)
            #    do_bwa_align(seqid, fastqhash[seqid], basename, basenameid, dbname, db)




    hdf.close()

    if args.telem is True:
        init_tel_threads2(connection_pool[dbname], tel_data_hash)


