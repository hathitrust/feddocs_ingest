# Take TX A&M's ndj file. 
# Identify records for govdocs and insert/update as appropriate
#
# Mostly ht_nightly.rb modified 
require 'marc'
require 'json'
require 'registry_record'
require 'source_record'
require 'dotenv'


Dotenv.load
Mongoid.load!("config/mongoid.yml", :development)
Mongo::Logger.logger.level = ::Logger::FATAL


total = 0
num_govdocs = 0
num_new_rr = 0
num_new_bib = 0

ORGCODE = "txcm"

def has_sudoc marc
  field_086 = marc['fields'].find {|f| f['086'] }
  if field_086 and 
    (field_086['086']['ind1'] == '0' or field_086['086']['a'] =~ /:/)
    return true 
  else
    return false
  end
end

def is_govdoc marc
  #if marc.nil?
  #  return false
  #end 
  fields = marc['fields'].find {|f| f['008'] }
  if fields.nil?
    return false
  end
  field_008 = fields['008']
  if field_008 =~ /^.{17}u.{10}f/ or has_sudoc(marc)
    true
  else
    false
  end
end


open(ARGV.shift,'r').each do | line | 
  total += 1
  line.chomp!

  new_src = SourceRecord.new
  new_src.org_code = ORGCODE 
  new_src.source = line 
  new_src.local_id = new_src.extract_local_id

  marc = JSON.parse line
  if new_src.source.nil?
    next
  end

  # pre-existing source record that has been updated
  src = SourceRecord.where(org_code: ORGCODE, local_id: new_src.local_id).first
  if src
    num_pre_existing += 1
    num_govdocs += 1
    #new enum chrons means new or updated regrec
    new_enum_chrons = new_src.enum_chrons - src.enum_chrons

    #trust that it's an improvement
    src.source = line
    src.source_blob = line
    src.in_registry = true
    src.save
    
    update_count += 1

    if new_enum_chrons 
      new_enum_chrons.each do |ec| 
        if regrec = RegistryRecord::cluster( src, ec)
          regrec.add_source(src)
        else
          regrec = RegistryRecord.new([src.source_id], ec, "TXCM update: #{infile}")
          num_new_rr += 1
        end
        regrec.save
      end
    end
    src.enum_chrons = new_src.enum_chrons
    src.save
    #without the recollate we are just hitting "last_modified" so it gets reindexed
    RegistryRecord.where(source_record_ids:src.source_id).no_timeout.each do |rr| 
      #rr.recollate #this blows up when dealing Serial Set or CFR
      rr.save
    end
  #new source record
  elsif is_govdoc(new_src.source) or 
        # we already think it is
        (new_src.oclc_resolved.count > 0 and 
         SourceRecord.in(oclc_resolved:new_src.oclc_resolved).where(deprecated_timestamp:{"$exists":0}).exists?)
    num_govdocs += 1
    new_src.source_blob = line
    new_src.in_registry = true
    new_src.save
    num_new_bib += 1

    if new_src.enum_chrons == []
      new_src.enum_chrons << ""
    end

    new_src.enum_chrons.each do |ec| 
      if regrec = RegistryRecord::cluster( new_src, ec)
        regrec.add_source(new_src)
      else
        regrec = RegistryRecord.new([new_src.source_id], ec, "TXCM update: #{infile}")
        num_new_rr += 1
      end
      regrec.save
    end
  #not an update or a new gov doc record
  else
   next
  end 
end # each record
puts "# of Govdoc records: #{num_govdocs}"
puts "# of new Registry Records: #{num_new_rr}"
puts "# of new Govdoc bib records: #{num_new_bib}"


