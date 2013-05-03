require 'rake/clean'

ERB = FileList['*.go.erb']
GO = ERB.ext
CLEAN.include(GO)

rule '.go' => '.go.erb' do |t|
  sh "erb #{t.source} > #{t.name}"
end

desc "Run tests"
task :test => GO do
  sh "go test"
end
