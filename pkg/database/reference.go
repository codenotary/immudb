package database

import (
	"fmt"

	"github.com/codenotary/immudb/pkg/api/schema"
)

//Reference ...
func (d *db) SetReference(req *schema.Reference) (*schema.TxMetadata, error) {
	/*d.Logger.Debugf("getReference options: %v", refOpts)
	return d.Store.GetReference(*refOpts)
	*/
	return nil, fmt.Errorf("Functionality not yet supported: %s", "Reference")
}

//Reference ...
func (d *db) GetReference(req *schema.KeyRequest) (*schema.Item, error) {
	/*d.Logger.Debugf("getReference options: %v", refOpts)
	return d.Store.GetReference(*refOpts)
	*/
	return nil, fmt.Errorf("Functionality not yet supported: %s", "Reference")
}

//VerifiableReference ...
func (d *db) VerifiableSetReference(req *schema.VerifiableReferenceRequest) (*schema.VerifiableTx, error) {
	//return d.Store.SafeReference(*safeRefOpts)
	return nil, fmt.Errorf("Functionality not yet supported: %s", "VerifiableReference")
}
