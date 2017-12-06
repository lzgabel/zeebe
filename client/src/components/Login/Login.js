import React from 'react';
import {Redirect} from 'react-router-dom';
import {getToken} from 'credentials';

import {login} from './service';

import {Message, Button, Input} from 'components';

import './Login.css';

export default class Login extends React.Component {
  constructor(props) {
    super(props);

    this.state = {
      username: '',
      password: '',
      error: false,
      redirect: false
    };
  }

  handleInputChange = ({target: {name, value}}) => {
    this.setState({
      [name]: value,
      error: false
    });
  }

  submit = async evt => {
    evt.preventDefault();

    const {username, password} = this.state;

    const token = await login(username, password);

    if(token) {
      this.setState({redirect: true});
    } else {
      this.setState({error: true});
    }
  }

  render() {
    const {username, password, redirect, error} = this.state;
    const locationState = this.props.location && this.props.location.state;

    if(redirect || getToken()) {
      return (
        <Redirect to={(locationState && locationState.from) || '/'} />
      );
    }

    return (
      <form  className='Login'>
        <h1 className="Login__heading">
          <span className='Login__brand-logo'></span>
          Camunda Optimize
        </h1>
        {error ? (<Message type='error' message='Could not log you in. Please check your username and password.'/>) : ('')}
        <div className='Login__controls'>
          <div className='Login__row'>
            <label className='Login__label visually-hidden' htmlFor='username'>Username</label>
            <Input className='Login__input' type='text' placeholder='Username' value={username} onChange={this.handleInputChange} name='username' autoFocus={true} />
          </div>
          <div className='Login__row'>
            <label className='Login__label visually-hidden' htmlFor='password'>Password</label>
            <Input className='Login__input' placeholder='Password' value={password} onChange={this.handleInputChange} type='password' name='password' reference={input => this.passwordField = input} />
          </div>
        </div>
        <Button type='submit' onClick={this.submit} className='Button--primary Button--blue Login__button'>Login</Button>
      </form>
    );
  }

  componentDidUpdate() {
    if(this.state.error) {
      this.passwordField.focus();
      this.passwordField.select();
    }
  }
}
