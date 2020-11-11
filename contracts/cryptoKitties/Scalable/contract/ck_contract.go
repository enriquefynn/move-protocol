// Code generated - DO NOT EDIT.
// This file is a generated binding and any manual changes will be lost.

package contract

import (
	"math/big"
	"strings"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/event"
)

// Reference imports to suppress errors if they are not otherwise used.
var (
	_ = big.NewInt
	_ = strings.NewReader
	_ = ethereum.NotFound
	_ = abi.U256
	_ = bind.Bind
	_ = common.Big1
	_ = types.BloomLookup
	_ = event.NewSubscription
)

// ContractABI is the input ABI used to generate the binding from.
const ContractABI = "[{\"constant\":false,\"inputs\":[{\"name\":\"_genes\",\"type\":\"uint256\"},{\"name\":\"_owner\",\"type\":\"address\"}],\"name\":\"createPromoKitty\",\"outputs\":[],\"payable\":false,\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"matron\",\"type\":\"address\"}],\"name\":\"giveBirth\",\"outputs\":[{\"name\":\"\",\"type\":\"address\"}],\"payable\":false,\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"matron\",\"type\":\"address\"},{\"name\":\"sire\",\"type\":\"address\"}],\"name\":\"breed\",\"outputs\":[],\"payable\":true,\"stateMutability\":\"payable\",\"type\":\"function\"}]"

// Contract is an auto generated Go binding around an Ethereum contract.
type Contract struct {
	ContractCaller     // Read-only binding to the contract
	ContractTransactor // Write-only binding to the contract
	ContractFilterer   // Log filterer for contract events
}

// ContractCaller is an auto generated read-only Go binding around an Ethereum contract.
type ContractCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// ContractTransactor is an auto generated write-only Go binding around an Ethereum contract.
type ContractTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// ContractFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type ContractFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// ContractSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type ContractSession struct {
	Contract     *Contract         // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// ContractCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type ContractCallerSession struct {
	Contract *ContractCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts   // Call options to use throughout this session
}

// ContractTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type ContractTransactorSession struct {
	Contract     *ContractTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts   // Transaction auth options to use throughout this session
}

// ContractRaw is an auto generated low-level Go binding around an Ethereum contract.
type ContractRaw struct {
	Contract *Contract // Generic contract binding to access the raw methods on
}

// ContractCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type ContractCallerRaw struct {
	Contract *ContractCaller // Generic read-only contract binding to access the raw methods on
}

// ContractTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type ContractTransactorRaw struct {
	Contract *ContractTransactor // Generic write-only contract binding to access the raw methods on
}

// NewContract creates a new instance of Contract, bound to a specific deployed contract.
func NewContract(address common.Address, backend bind.ContractBackend) (*Contract, error) {
	contract, err := bindContract(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &Contract{ContractCaller: ContractCaller{contract: contract}, ContractTransactor: ContractTransactor{contract: contract}, ContractFilterer: ContractFilterer{contract: contract}}, nil
}

// NewContractCaller creates a new read-only instance of Contract, bound to a specific deployed contract.
func NewContractCaller(address common.Address, caller bind.ContractCaller) (*ContractCaller, error) {
	contract, err := bindContract(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &ContractCaller{contract: contract}, nil
}

// NewContractTransactor creates a new write-only instance of Contract, bound to a specific deployed contract.
func NewContractTransactor(address common.Address, transactor bind.ContractTransactor) (*ContractTransactor, error) {
	contract, err := bindContract(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &ContractTransactor{contract: contract}, nil
}

// NewContractFilterer creates a new log filterer instance of Contract, bound to a specific deployed contract.
func NewContractFilterer(address common.Address, filterer bind.ContractFilterer) (*ContractFilterer, error) {
	contract, err := bindContract(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &ContractFilterer{contract: contract}, nil
}

// bindContract binds a generic wrapper to an already deployed contract.
func bindContract(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(ContractABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Contract *ContractRaw) Call(opts *bind.CallOpts, result interface{}, method string, params ...interface{}) error {
	return _Contract.Contract.ContractCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Contract *ContractRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Contract.Contract.ContractTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Contract *ContractRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Contract.Contract.ContractTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Contract *ContractCallerRaw) Call(opts *bind.CallOpts, result interface{}, method string, params ...interface{}) error {
	return _Contract.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Contract *ContractTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Contract.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Contract *ContractTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Contract.Contract.contract.Transact(opts, method, params...)
}

// Breed is a paid mutator transaction binding the contract method 0xcfb4246e.
//
// Solidity: function breed(address matron, address sire) returns()
func (_Contract *ContractTransactor) Breed(opts *bind.TransactOpts, matron common.Address, sire common.Address) (*types.Transaction, error) {
	return _Contract.contract.Transact(opts, "breed", matron, sire)
}

// Breed is a paid mutator transaction binding the contract method 0xcfb4246e.
//
// Solidity: function breed(address matron, address sire) returns()
func (_Contract *ContractSession) Breed(matron common.Address, sire common.Address) (*types.Transaction, error) {
	return _Contract.Contract.Breed(&_Contract.TransactOpts, matron, sire)
}

// Breed is a paid mutator transaction binding the contract method 0xcfb4246e.
//
// Solidity: function breed(address matron, address sire) returns()
func (_Contract *ContractTransactorSession) Breed(matron common.Address, sire common.Address) (*types.Transaction, error) {
	return _Contract.Contract.Breed(&_Contract.TransactOpts, matron, sire)
}

// CreatePromoKitty is a paid mutator transaction binding the contract method 0x56129134.
//
// Solidity: function createPromoKitty(uint256 _genes, address _owner) returns()
func (_Contract *ContractTransactor) CreatePromoKitty(opts *bind.TransactOpts, _genes *big.Int, _owner common.Address) (*types.Transaction, error) {
	return _Contract.contract.Transact(opts, "createPromoKitty", _genes, _owner)
}

// CreatePromoKitty is a paid mutator transaction binding the contract method 0x56129134.
//
// Solidity: function createPromoKitty(uint256 _genes, address _owner) returns()
func (_Contract *ContractSession) CreatePromoKitty(_genes *big.Int, _owner common.Address) (*types.Transaction, error) {
	return _Contract.Contract.CreatePromoKitty(&_Contract.TransactOpts, _genes, _owner)
}

// CreatePromoKitty is a paid mutator transaction binding the contract method 0x56129134.
//
// Solidity: function createPromoKitty(uint256 _genes, address _owner) returns()
func (_Contract *ContractTransactorSession) CreatePromoKitty(_genes *big.Int, _owner common.Address) (*types.Transaction, error) {
	return _Contract.Contract.CreatePromoKitty(&_Contract.TransactOpts, _genes, _owner)
}

// GiveBirth is a paid mutator transaction binding the contract method 0x816b6714.
//
// Solidity: function giveBirth(address matron) returns(address)
func (_Contract *ContractTransactor) GiveBirth(opts *bind.TransactOpts, matron common.Address) (*types.Transaction, error) {
	return _Contract.contract.Transact(opts, "giveBirth", matron)
}

// GiveBirth is a paid mutator transaction binding the contract method 0x816b6714.
//
// Solidity: function giveBirth(address matron) returns(address)
func (_Contract *ContractSession) GiveBirth(matron common.Address) (*types.Transaction, error) {
	return _Contract.Contract.GiveBirth(&_Contract.TransactOpts, matron)
}

// GiveBirth is a paid mutator transaction binding the contract method 0x816b6714.
//
// Solidity: function giveBirth(address matron) returns(address)
func (_Contract *ContractTransactorSession) GiveBirth(matron common.Address) (*types.Transaction, error) {
	return _Contract.Contract.GiveBirth(&_Contract.TransactOpts, matron)
}
