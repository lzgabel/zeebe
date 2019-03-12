/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a commercial license.
 * You may not use this file except in compliance with the commercial license.
 */

import styled from 'styled-components';
import {INCIDENTS_BAR_HEIGHT} from 'modules/constants.js';
import {Colors} from 'modules/theme';
import withStrippedProps from 'modules/utils/withStrippedProps';
import {ReactComponent as Down} from 'modules/components/Icon/down.svg';

export const IncidentsBar = styled.button`
  display: flex;
  align-items: center;
  position: relative;
  z-index: 5;

  height: ${INCIDENTS_BAR_HEIGHT}px;
  padding: 0 20px 0 17px;
  font-size: 15px;
  font-weight: bold;
  opacity: 1;

  background-color: ${Colors.incidentsAndErrors};
  color: #ffffff;

  cursor: pointer;
`;

export const Arrow = styled(withStrippedProps(['isFlipped'])(Down))`
  margin-right: 11px;
  position: relative;

  transform: ${props => (props.isFlipped ? 'none' : 'rotate(-90deg)')};
  left: ${props => (props.isFlipped ? '2px' : '0')};
`;
