package pgproxy

import "testing"

func Test_dbProxy_InitFromURL(t *testing.T) {
	type args struct {
		address string
	}
	tests := []struct {
		name    string
		proxy   *dbProxy
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.proxy.InitFromURL(tt.args.address); (err != nil) != tt.wantErr {
				t.Errorf("dbProxy.InitFromURL() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
