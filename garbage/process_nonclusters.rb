# Rebuild the registry using cluster files
# Process clusters with scores below .9 as separate source items
require 'registry_record'
require 'source_record'
require 'json'
require 'dotenv'
require 'pp'

Mongoid.load!("config/mongoid.yml", :development)

CUTOFF = 0.9 #dupe scores >= than .9 are ignored
fout = open("unmatched.txt", 'w')

clusters = open(ARGV.shift)
gme = open(ARGV.shift)
enumchrons = {}
gme.each do | line |
  l = line.chomp.split(/\t/)
  enumchrons[l[0]] ||= [l[1],[]]
  enumchrons[l[0]][1] << l[2]
end
match_count = 0
nomatch_count = 0
line_number = 0
clusters.each do | line | 
  line_number += 1
  c = line.chomp.split(/\t/)
  if c[1].to_f >= CUTOFF 
    next
  end

  gdids = c[2].split(/,/)
  gdids.each do | gdid |
    # skip it if it somehow got into the registry already
    r = RegistryRecord.where(source_record_ids: enumchrons[gdid][0]).first
    if r
      next
    end
    source = SourceRecord.where(source_id: enumchrons[gdid][0]).first
    enumchrons[gdid][1].each do | ec |
      if ec == 'NULL' 
        ec = ''
      end
      regrec = RegistryRecord::cluster(source, ec)
      if regrec
        match_count += 1
        regrec.add_source source
      else
        nomatch_count += 1
        regrec = RegistryRecord.new([enumchrons[gdid][0]], ec, 'low dupe score')
        #fout.write("#{enumchrons[gdid][0]}\t#{ec}")
      end
      regrec.save()
    end

  end
  if line_number % 10000 == 0
    print "\line number #{line_number}"
  end
end
puts match_count
puts nomatch_count

