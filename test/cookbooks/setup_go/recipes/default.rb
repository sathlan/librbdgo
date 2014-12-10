directory '/opt/go/src/github.com/sathlan' do
  recursive true
end

link "/opt/go/src/github.com/sathlan/librbdgo" do
  to "/vagrant"
end
