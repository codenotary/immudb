package database

import (
	"fmt"
	"github.com/codenotary/immudb/pkg/api/schema"
)

//Reference ...
func (d *db) Reference(refOpts *schema.ReferenceOptions) (index *schema.Root, err error) {
	/*d.Logger.Debugf("getReference options: %v", refOpts)
	return d.Store.GetReference(*refOpts)
	*/
	return nil, fmt.Errorf("Functionality not yet supported: %s", "Reference")
}

//Reference ...
func (d *db) GetReference(refOpts *schema.Key) (item *schema.Item, err error) {
	/*d.Logger.Debugf("getReference options: %v", refOpts)
	return d.Store.GetReference(*refOpts)
	*/
	return nil, fmt.Errorf("Functionality not yet supported: %s", "Reference")
}

//SafeReference ...
func (d *db) SafeReference(safeRefOpts *schema.SafeReferenceOptions) (proof *schema.Proof, err error) {
	//return d.Store.SafeReference(*safeRefOpts)
	return nil, fmt.Errorf("Functionality not yet supported: %s", "SafeReference")
}
