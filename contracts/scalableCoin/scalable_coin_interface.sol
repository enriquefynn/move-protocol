pragma solidity >0.4.24;
contract STokenInterface {
  function totalSupply() public view returns (uint);
  function newAccount() public payable returns (AccountInterface, uint);
  function newAccountFor(address _forAddr) public payable returns (AccountInterface, uint);
  event CreatedAccount(address account, uint id);
}

contract AccountInterface {
  function balance() public view returns (uint);
  function allowance(address _spender) public view returns (uint);
  function transfer(AccountInterface _to, uint _tokens) public returns (bool);
  function approve(address _spender, uint _tokens) public returns (bool);
  function transferFrom(AccountInterface _to, uint _tokens) public returns (bool);
  function debit(uint _tokens, uint _proof) public returns (bool);
  function moveTo(uint shardId) public;
  event Transfer(AccountInterface indexed _to, uint _tokens);
  event Approval(address indexed _spender, uint _tokens);
  event CreatedAccount(address account, uint id);
}