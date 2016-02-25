# Rebuild the registry using cluster files
# Process clusters with scores below .9 as separate source items
require 'registry_record'
require 'source_record'
require 'json'
require 'dotenv'
require 'pp'

Mongoid.load!("config/mongoid.yml", :development)

CUTOFF = 0.9 #dupe scores >= than .9 are ignored

clusters = open(ARGV.shift)
gme = open(ARGV.shift)
enumchrons = {}
gme.each do | line |
  l = line.chomp.split(/\t/)
  enumchrons[l[0]] ||= [l[1],[]]
  enumchrons[l[0]][1] << l[2]
end

count = 0
line_number = 0
clusters.each do | line | 
  line_number += 1
  c = line.chomp.split(/\t/)
  if c[1].to_f >= CUTOFF 
    next
  end

  gdids = c[2].split(/,/)
  gdids.each do | gdid |
    source = SourceRecord.where(source_id: enumchrons[gd][0]).first
    enumchrons[gd][1].each do | ec |
      if ec == 'NULL' 
        ec = ''
      end
      regrec = RegistryRecord::cluster(source, ec)
      if regrec
        regrec.add_source source
      else
        regrec = RegistryRecord.new([enumchrons[gd][0]], ec, 'low dupe score')
      end
      regrec.save()
    end

    count += 1
    if count % 10000 == 0
      print "\rcount #{count}"
    end
  end
end
puts count

