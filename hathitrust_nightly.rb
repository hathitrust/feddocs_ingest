# update preexisting HathiTrust records
# add new HathiTrust records existing Registry Records
# create new Registry Records if no match can be found
# we are NOT recalculating clustering!!!
#
require 'registry_record'
require 'source_record'
require 'json'
require 'dotenv'
require 'pp'
require 'htph'

Mongoid.load!("config/mongoid.yml", :development)

ORGCODE = 'miaahdl'

fin = ARGV.shift
updates = open(fin)
count = 0
updates.each do | line | 
  line.chomp!
  marc = JSON.parse line

  #is this an edit, do we already have it?
  htid = marc['fields'].find {|f| f['001'] }['001']
  field_008 = marc['fields'].find {|f| f['008'] }['008']

  enum_chrons = []
  marc['fields'].select {|f| f['974']}.each do | f | 
    enum_chrons << f['974']['subfields'].select {|sf| sf['z']}.collect { |z| HTPH::Hathinormalize.enumc(z['z']) }
  end
  enum_chrons.flatten!.uniq!

  # pre-existing source record that has been updated
  src = SourceRecord.where(local_id: htid, org_code: ORGCODE).first
  if src
    puts src[:local_id]
    #trust that it's an improvement
    src.source = line
    src.save
    puts src.source_id    
    count += 1 

    #new enum chrons means new or updated regrec
    new_enum_chrons = enum_chrons - src.enum_chrons
    if new_enum_chrons 
      new_enum_chrons.each do  |ec| 
        if regrec = RegistryRecord::cluster( src, ec)
          regrec.source_record_ids << src.source_id 
        else
          regrec = RegistryRecord.new([src.source_id], ec, "HT update: #{fin}")
        end
        regrec.save
      end
    end
  #new source record
  elsif field_008 =~ /^.{17}u.{10}f/ #is it a gov doc?
    src = SourceRecord.new
    src.source = line
    src.org_code = ORGCODE
    src.local_id = htid
    src.enum_chrons = enum_chrons
    src.save
    count += 1 

    if enum_chrons == []
      enum_chrons << ""
    end

    enum_chrons.each do |ec| 
      if regrec = RegistryRecord::cluster( src, ec)
        regrec.source_record_ids << src.source_id
      else
        regrec = RegistryRecord.new([src.source_id], ec, "HT update: #{fin}")
      end
      regrec.save
    end
  #not an update or a new gov doc record
  else
    next
  end

  #update all RegistryRecords tied to this record
  RegistryRecord.where(source_record_ids: src.source_id).each do | r | 
    r.recollate
    r.save
  end


end
puts count

